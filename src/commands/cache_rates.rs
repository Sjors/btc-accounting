use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{Datelike, NaiveDate, Utc};
use csv::StringRecord;
use reqwest::blocking::{Client, Response};
use zip::ZipArchive;

use crate::common::{
    AppConfig, KRAKEN_INTERVALS_MINUTES, build_http_client, fetch_candles_since,
    parse_candle_interval_minutes,
};
use crate::exchange_rate::{CACHE_DIR, cache_key, cache_path, load_disk_cache, save_disk_cache};

pub const SUBCOMMAND_NAME: &str = "cache-rates";
pub const USAGE: &str = "usage: btc_fiat_value cache-rates [--vwap] [--candle <minutes>] <year>";

const KRAKEN_DAILY_INTERVAL_MINUTES: u32 = 1_440;
const QUARTERLY_ARCHIVE_FIRST_YEAR: i32 = 2023;
// Kraken archive references:
// - OHLC REST retention and daily 1440 candles: https://docs.kraken.com/api/docs/rest-api/get-ohlc-data/
// - Downloadable OHLCVT archive landing page: https://support.kraken.com/articles/360047124832-downloadable-historical-ohlcvt-open-high-low-close-volume-trades-data
// If Kraken changes the Google Drive ids or stops publishing them there, update these constants.
const COMPLETE_OHLCVT_ARCHIVE_FILE_ID: &str = "1ptNqWYidLkhb2VAKuLCxmp2OXEfGO-AP";
const QUARTERLY_OHLCVT_ARCHIVE_FOLDER_ID: &str = "15RSlNuW_h0kVM8or8McOGOMfHeBFvFGI";
// Kraken time-and-sales archive references:
// - Downloadable trade archive landing page: https://support.kraken.com/articles/360047543791-downloadable-historical-market-data-time-and-sales-
// - OHLC REST docs for the live daily VWAP path: https://docs.kraken.com/api/docs/rest-api/get-ohlc-data/
// If Kraken changes the Google Drive ids or stops publishing them there, update these constants.
const COMPLETE_TRADE_ARCHIVE_FILE_ID: &str = "10zh3tDpqANYvVtYVgczwVz3UZFRUb1el";
const QUARTERLY_TRADE_ARCHIVE_FOLDER_ID: &str = "188O9xQjZTythjyLNes_5zfMEFaMbTT22";

#[derive(Debug, Eq, PartialEq)]
pub struct CacheRatesArgs {
    pub year: i32,
    pub use_vwap_archive: bool,
    pub candle_override_minutes: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DriveFile {
    id: String,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedArchive {
    archive_path: PathBuf,
    archive_file_name: String,
    extracted_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedArchiveFile {
    file: DriveFile,
    archive: PreparedArchive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArchiveCoverage {
    first: i64,
    last: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArchiveBackfillMode {
    Midpoint,
    Vwap,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CacheWriteStats {
    inserted: usize,
    replaced: usize,
    skipped: usize,
}

impl CacheWriteStats {
    fn record(&mut self, outcome: CacheWriteOutcome) {
        match outcome {
            CacheWriteOutcome::Inserted => self.inserted += 1,
            CacheWriteOutcome::Replaced => self.replaced += 1,
            CacheWriteOutcome::Skipped => self.skipped += 1,
        }
    }

    fn absorb(&mut self, other: CacheWriteStats) {
        self.inserted += other.inserted;
        self.replaced += other.replaced;
        self.skipped += other.skipped;
    }

    fn written(&self) -> usize {
        self.inserted + self.replaced
    }

    fn total(&self) -> usize {
        self.inserted + self.replaced + self.skipped
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheWriteOutcome {
    Inserted,
    Replaced,
    Skipped,
}

pub fn run(args: CacheRatesArgs) -> Result<()> {
    let _temp_cache = TempCacheGuard::new()?;
    let config = AppConfig::from_env()?;
    let archive_pair = archive_pair_name(&config.kraken_pair)?;
    let archive_mode = if args.use_vwap_archive {
        ArchiveBackfillMode::Vwap
    } else {
        ArchiveBackfillMode::Midpoint
    };
    let target_interval_minutes = target_interval_minutes(&args, &config)?;
    let now = Utc::now();
    let (start_ts, end_ts) = closed_interval_year_bounds(args.year, now, target_interval_minutes)?;

    let mut missing_starts =
        expected_interval_starts(start_ts, end_ts, target_interval_minutes);
    let mut cache = load_disk_cache();
    let existing_cache_count = count_cached_starts(
        &missing_starts,
        &cache,
        &config.kraken_pair,
        target_interval_minutes,
    );
    if archive_mode == ArchiveBackfillMode::Midpoint {
        drop_cached_starts(
            &mut missing_starts,
            &cache,
            &config.kraken_pair,
            target_interval_minutes,
        );
    }
    let full_span_quarters = missing_quarters(&missing_starts);
    let quarterly_files = if !missing_starts.is_empty() && args.year >= QUARTERLY_ARCHIVE_FIRST_YEAR {
        let archive_client = build_http_client("Kraken archive", None)?;
        eprintln!(
            "Fetching Kraken {} archive listing for {}...",
            archive_mode.archive_label(),
            args.year,
        );
        Some(fetch_quarterly_archive_files(
            &archive_client,
            archive_mode.quarterly_archive_folder_id(),
            archive_mode.quarterly_archive_prefix(),
            archive_mode.archive_label(),
        )?)
    } else {
        None
    };
    let has_full_quarterly_coverage = quarterly_files
        .as_ref()
        .and_then(|files| {
            resolve_quarterly_archive_files(
                files,
                archive_mode.quarterly_archive_prefix(),
                args.year,
                &full_span_quarters,
            )
        })
        .is_some();
    let api_can_cover_entire_span = missing_starts.len() <= 720;
    let should_fetch_api = match archive_mode {
        ArchiveBackfillMode::Midpoint => true,
        ArchiveBackfillMode::Vwap => api_can_cover_entire_span || has_full_quarterly_coverage,
    };

    let mut recent_stats = CacheWriteStats::default();
    if should_fetch_api {
        let kraken_client = build_http_client("clearnet Kraken", None)?;
        eprintln!(
            "Fetching Kraken OHLC API rows at {}-minute resolution for {}...",
            target_interval_minutes,
            args.year,
        );
        let candles = fetch_candles_since(
            &kraken_client,
            &config,
            target_interval_minutes,
            start_ts,
        )?;

        for candle in candles {
            if candle.time < start_ts || candle.time >= end_ts {
                continue;
            }
            if !should_store_api_candle(archive_mode, &missing_starts, candle.time) {
                continue;
            }

            recent_stats.record(store_cache_value(
                &mut cache,
                &config.kraken_pair,
                target_interval_minutes,
                candle.time,
                candle.vwap,
            ));
            missing_starts.remove(&candle.time);
        }
    }

    let mut archive_stats = CacheWriteStats::default();
    let mut fallback_stats = CacheWriteStats::default();
    let mut used_complete_archive_fallback = false;
    let mut prepared_archives = Vec::new();
    if !missing_starts.is_empty() {
        let archive_client = build_http_client("Kraken archive", None)?;

        let needed_quarters = missing_quarters(&missing_starts);
        let quarterly_backfill_files = quarterly_files
            .as_ref()
            .and_then(|files| {
                resolve_quarterly_archive_files(
                    files,
                    archive_mode.quarterly_archive_prefix(),
                    args.year,
                    &needed_quarters,
                )
            });

        if let Some(files) = quarterly_backfill_files {
            for file in files {
                let prepared_archive = prepare_archive_for_backfill(
                    &archive_client,
                    &file,
                    &archive_pair,
                    end_ts,
                    target_interval_minutes,
                    archive_mode,
                )?;
                archive_stats.absorb(read_prepared_archive(
                    &prepared_archive,
                    &file,
                    &config.kraken_pair,
                    &archive_pair,
                    start_ts,
                    end_ts,
                    target_interval_minutes,
                    None,
                    &mut missing_starts,
                    &mut cache,
                    archive_mode,
                )?);
                prepared_archives.push(PreparedArchiveFile {
                    file,
                    archive: prepared_archive,
                });
            }
        } else {
            used_complete_archive_fallback = true;
            let file = DriveFile {
                id: archive_mode.complete_archive_file_id().to_owned(),
                name: archive_mode.complete_archive_name().to_owned(),
            };
            let prepared_archive = prepare_archive_for_backfill(
                &archive_client,
                &file,
                &archive_pair,
                end_ts,
                target_interval_minutes,
                archive_mode,
            )?;
            archive_stats.absorb(read_prepared_archive(
                &prepared_archive,
                &file,
                &config.kraken_pair,
                &archive_pair,
                start_ts,
                end_ts,
                target_interval_minutes,
                None,
                &mut missing_starts,
                &mut cache,
                archive_mode,
            )?);
            prepared_archives.push(PreparedArchiveFile {
                file,
                archive: prepared_archive,
            });
        }
    }

    if archive_mode == ArchiveBackfillMode::Vwap && !missing_starts.is_empty() {
        for fallback_interval_minutes in fallback_trade_intervals(target_interval_minutes) {
            let missing_before = missing_starts.len();
            eprintln!(
                "Warning: exact {}-minute trade VWAP was unavailable for {} interval(s); falling back to {}-minute trade VWAP for those gaps.",
                target_interval_minutes,
                missing_before,
                fallback_interval_minutes,
            );
            let mut level_stats = CacheWriteStats::default();
            for prepared_archive in &prepared_archives {
                level_stats.absorb(read_prepared_archive(
                    &prepared_archive.archive,
                    &prepared_archive.file,
                    &config.kraken_pair,
                    &archive_pair,
                    start_ts,
                    end_ts,
                    target_interval_minutes,
                    Some(fallback_interval_minutes),
                    &mut missing_starts,
                    &mut cache,
                    archive_mode,
                )?);
            }
            fallback_stats.absorb(level_stats);
            if level_stats.written() > 0 {
                eprintln!(
                    "Filled {} {}-minute gap(s) using {}-minute trade VWAP.",
                    level_stats.written(),
                    target_interval_minutes,
                    fallback_interval_minutes,
                );
            }
            if missing_starts.is_empty() {
                break;
            }
        }
    }

    if !missing_starts.is_empty() {
        let first_missing = missing_starts
            .iter()
            .next()
            .copied()
            .context("missing intervals should not be empty")?;
        bail!(
            "cache is incomplete for {}: {} {}-minute candle(s) still missing, starting at {}",
            args.year,
            missing_starts.len(),
            target_interval_minutes,
            format_interval_start(first_missing, target_interval_minutes)?
        );
    }

    save_disk_cache(&cache)?;

    let inserted_count = recent_stats.inserted + archive_stats.inserted + fallback_stats.inserted;
    let replaced_count = recent_stats.replaced + archive_stats.replaced + fallback_stats.replaced;
    let skipped_count = if archive_mode == ArchiveBackfillMode::Midpoint {
        existing_cache_count
    } else {
        recent_stats.skipped + archive_stats.skipped + fallback_stats.skipped
    };
    let total_count = inserted_count + replaced_count + skipped_count;
    eprintln!(
        "Cached {total_count} {}-minute rate(s) for {} in {}.",
        target_interval_minutes,
        args.year,
        cache_path().display()
    );
    eprintln!("Inserted cache entries: {inserted_count}");
    eprintln!("Replaced existing cache entries: {replaced_count}");
    eprintln!("Skipped existing cache entries: {skipped_count}");
    eprintln!("Kraken OHLC API rows: {}", recent_stats.written());
    eprintln!("{}: {}", archive_mode.summary_label(), archive_stats.written());
    if fallback_stats.written() > 0 {
        eprintln!(
            "Trade archive larger-candle fallback rows: {}",
            fallback_stats.written()
        );
    }
    if used_complete_archive_fallback
        && (archive_stats.total() > 0 || fallback_stats.total() > 0)
    {
        eprintln!(
            "Quarterly {} archives were unavailable for {}, so the command fell back to the complete {} archive.",
            archive_mode.archive_label(),
            args.year,
            archive_mode.archive_label(),
        );
    }
    if archive_stats.total() > 0 || fallback_stats.total() > 0 {
        match archive_mode {
            ArchiveBackfillMode::Midpoint => eprintln!(
                "Archive-backed rows use the daily (open + close) / 2 midpoint because Kraken's OHLCVT CSV does not include VWAP. Re-run with --vwap for exact trade-derived VWAP."
            ),
            ArchiveBackfillMode::Vwap => eprintln!(
                "Archive-backed rows use exact {}-minute VWAP computed from Kraken's time-and-sales trade archive, overwriting any existing cache entries for that year and interval.",
                target_interval_minutes
            ),
        }
    }

    Ok(())
}

pub fn parse_args_from<I>(args: I, usage: &str) -> Result<CacheRatesArgs>
where
    I: IntoIterator<Item = String>,
{
    let mut year = None;
    let mut use_vwap_archive = false;
    let mut candle_override_minutes = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        if arg == "--vwap" {
            use_vwap_archive = true;
            continue;
        }
        if arg == "--candle" {
            let value = args.next().ok_or_else(|| anyhow!("{usage}"))?;
            candle_override_minutes = Some(parse_candle_interval_minutes(&value, "--candle")?);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--candle=") {
            candle_override_minutes = Some(parse_candle_interval_minutes(value, "--candle")?);
            continue;
        }

        if year.is_some() {
            bail!("{usage}");
        }
        year = Some(arg);
    }

    if candle_override_minutes.is_some() && !use_vwap_archive {
        bail!("--candle requires --vwap\n\n{usage}");
    }

    let year = year.ok_or_else(|| anyhow!("{usage}"))?;
    let year = year
        .parse::<i32>()
        .with_context(|| format!("invalid year: {year}"))?;

    Ok(CacheRatesArgs {
        year,
        use_vwap_archive,
        candle_override_minutes,
    })
}

fn target_interval_minutes(args: &CacheRatesArgs, config: &AppConfig) -> Result<u32> {
    if !args.use_vwap_archive {
        return Ok(KRAKEN_DAILY_INTERVAL_MINUTES);
    }

    Ok(args
        .candle_override_minutes
        .or(config.default_candle_minutes)
        .unwrap_or(KRAKEN_DAILY_INTERVAL_MINUTES))
}

fn closed_interval_year_bounds(
    year: i32,
    now: chrono::DateTime<Utc>,
    interval_minutes: u32,
) -> Result<(i64, i64)> {
    let year_start = NaiveDate::from_ymd_opt(year, 1, 1)
        .ok_or_else(|| anyhow!("invalid year: {year}"))?;
    let next_year_start = NaiveDate::from_ymd_opt(year + 1, 1, 1)
        .ok_or_else(|| anyhow!("invalid year: {year}"))?;
    let year_start_ts = midnight_utc_timestamp(year_start);
    let next_year_start_ts = midnight_utc_timestamp(next_year_start);
    let interval_seconds = i64::from(interval_minutes) * 60;

    if year > now.year() {
        bail!("year {year} is in the future");
    }

    let year_end_exclusive = if year == now.year() {
        now.timestamp().div_euclid(interval_seconds) * interval_seconds
    } else {
        next_year_start_ts
    };

    if year_start_ts >= year_end_exclusive {
        bail!(
            "year {year} has no closed UTC {}-minute candles yet",
            interval_minutes
        );
    }

    Ok((year_start_ts, year_end_exclusive.min(next_year_start_ts)))
}

fn expected_interval_starts(start_ts: i64, end_ts: i64, interval_minutes: u32) -> BTreeSet<i64> {
    let mut starts = BTreeSet::new();
    let interval_seconds = i64::from(interval_minutes) * 60;
    let mut ts = start_ts;
    while ts < end_ts {
        starts.insert(ts);
        ts += interval_seconds;
    }
    starts
}

fn count_cached_starts(
    starts: &BTreeSet<i64>,
    cache: &HashMap<String, f64>,
    kraken_pair: &str,
    interval_minutes: u32,
) -> usize {
    starts
        .iter()
        .filter(|start| cache.contains_key(&cache_key(kraken_pair, interval_minutes, **start)))
        .count()
}

fn should_store_api_candle(
    archive_mode: ArchiveBackfillMode,
    missing_starts: &BTreeSet<i64>,
    candle_start: i64,
) -> bool {
    archive_mode == ArchiveBackfillMode::Vwap || missing_starts.contains(&candle_start)
}

fn drop_cached_starts(
    missing_starts: &mut BTreeSet<i64>,
    cache: &HashMap<String, f64>,
    kraken_pair: &str,
    interval_minutes: u32,
) {
    missing_starts.retain(|start| {
        !cache.contains_key(&cache_key(kraken_pair, interval_minutes, *start))
    });
}

fn store_cache_value(
    cache: &mut HashMap<String, f64>,
    kraken_pair: &str,
    interval_minutes: u32,
    timestamp: i64,
    value: f64,
) -> CacheWriteOutcome {
    let key = cache_key(kraken_pair, interval_minutes, timestamp);
    let normalized = normalize_fiat_rate(value);
    match cache.get(&key) {
        Some(existing) if same_fiat_cent(*existing, value) => {
            if *existing != normalized {
                cache.insert(key, normalized);
            }
            CacheWriteOutcome::Skipped
        }
        Some(_) => {
            cache.insert(key, normalized);
            CacheWriteOutcome::Replaced
        }
        None => {
            cache.insert(key, normalized);
            CacheWriteOutcome::Inserted
        }
    }
}

fn same_fiat_cent(left: f64, right: f64) -> bool {
    round_fiat_cents(left) == round_fiat_cents(right)
}

fn normalize_fiat_rate(value: f64) -> f64 {
    round_fiat_cents(value) as f64 / 100.0
}

fn round_fiat_cents(value: f64) -> i64 {
    (value * 100.0).round() as i64
}

fn missing_quarters(missing_days: &BTreeSet<i64>) -> BTreeSet<u32> {
    let mut quarters = BTreeSet::new();
    for ts in missing_days {
        let date = chrono::DateTime::from_timestamp(*ts, 0)
            .expect("daily candle timestamp should be valid")
            .date_naive();
        quarters.insert((date.month0() / 3) + 1);
    }
    quarters
}

fn resolve_quarterly_archive_files(
    quarterly_files: &HashMap<String, DriveFile>,
    file_prefix: &str,
    year: i32,
    needed_quarters: &BTreeSet<u32>,
) -> Option<Vec<DriveFile>> {
    let mut files = Vec::with_capacity(needed_quarters.len());

    for quarter in needed_quarters {
        let name = format!("{file_prefix}Q{quarter}_{year}.zip");
        files.push(quarterly_files.get(&name)?.clone());
    }

    Some(files)
}

fn prepare_archive_for_backfill(
    client: &Client,
    file: &DriveFile,
    archive_pair: &str,
    end_ts: i64,
    target_interval_minutes: u32,
    archive_mode: ArchiveBackfillMode,
) -> Result<PreparedArchive> {
    let archive_path = archive_download_path(&file.name);
    if let Some(parent) = archive_path.parent() {
        fs::create_dir_all(parent)?;
    }

    ensure_archive_downloaded(
        client,
        file,
        &archive_path,
        archive_mode,
        archive_pair,
        end_ts,
        target_interval_minutes,
    )
}

fn read_prepared_archive(
    prepared_archive: &PreparedArchive,
    _file: &DriveFile,
    kraken_pair: &str,
    _archive_pair: &str,
    start_ts: i64,
    end_ts: i64,
    target_interval_minutes: u32,
    trade_source_interval_minutes: Option<u32>,
    missing_starts: &mut BTreeSet<i64>,
    cache: &mut HashMap<String, f64>,
    archive_mode: ArchiveBackfillMode,
) -> Result<CacheWriteStats> {
    eprintln!(
        "Reading extracted {} data from {}...",
        archive_mode.archive_label(),
        prepared_archive.extracted_path.display(),
    );
    read_archive_zip(
        &prepared_archive.extracted_path,
        kraken_pair,
        start_ts,
        end_ts,
        target_interval_minutes,
        trade_source_interval_minutes,
        missing_starts,
        cache,
        archive_mode,
    )
}

fn read_archive_zip(
    path: &Path,
    kraken_pair: &str,
    start_ts: i64,
    end_ts: i64,
    target_interval_minutes: u32,
    trade_source_interval_minutes: Option<u32>,
    missing_starts: &mut BTreeSet<i64>,
    cache: &mut HashMap<String, f64>,
    archive_mode: ArchiveBackfillMode,
) -> Result<CacheWriteStats> {
    let csv_file = File::open(path)
        .with_context(|| format!("failed to open extracted archive data {}", path.display()))?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(csv_file);
    let entry_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("archive data");

    match archive_mode {
        ArchiveBackfillMode::Midpoint => read_ohlcvt_daily_csv(
            &mut reader,
            &entry_name,
            kraken_pair,
            start_ts,
            end_ts,
            target_interval_minutes,
            missing_starts,
            cache,
        ),
        ArchiveBackfillMode::Vwap => read_trade_interval_csv(
            &mut reader,
            &entry_name,
            kraken_pair,
            start_ts,
            end_ts,
            trade_source_interval_minutes.unwrap_or(target_interval_minutes),
            target_interval_minutes,
            missing_starts,
            cache,
        ),
    }
}

fn ensure_archive_entry_extracted(
    archive_path: &Path,
    archive_file_name: &str,
    archive_pair: &str,
    archive_mode: ArchiveBackfillMode,
) -> Result<PathBuf> {
    let expected_entry_name = archive_mode.entry_name(archive_pair);
    let extracted_path = extracted_archive_entry_path(archive_file_name, &expected_entry_name);
    if extracted_path.exists() {
        return Ok(extracted_path);
    }

    if let Some(parent) = extracted_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let archive_file = File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    let mut archive = ZipArchive::new(archive_file).with_context(|| {
        format!("failed to read ZIP archive {}", archive_path.display())
    })?;
    let entry_name = resolve_archive_entry_name(&mut archive, &expected_entry_name)?;
    eprintln!(
        "Extracting {} from {} into {}...",
        entry_name,
        archive_path.display(),
        temp_cache_dir().display(),
    );
    let mut csv_file = archive
        .by_name(&entry_name)
        .with_context(|| format!("archive does not contain {entry_name}"))?;
    let temp_path = temp_extracted_archive_entry_path(archive_file_name, &expected_entry_name);
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut output = File::create(&temp_path)
        .with_context(|| format!("failed to create {}", temp_path.display()))?;
    io::copy(&mut csv_file, &mut output)
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    fs::rename(&temp_path, &extracted_path)
        .with_context(|| format!("failed to move extracted data into {}", extracted_path.display()))?;
    Ok(extracted_path)
}

fn extracted_csv_timestamp_bounds(path: &Path, entry_name: &str) -> Result<Option<ArchiveCoverage>> {
    let csv_file = File::open(path)
        .with_context(|| format!("failed to open extracted archive data {}", path.display()))?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(csv_file);
    let mut first = None;
    let mut last = None;

    for record in reader.records() {
        let record = record.with_context(|| format!("failed to parse a row in {entry_name}"))?;
        let timestamp = parse_archive_timestamp(&record, &entry_name)?;
        first.get_or_insert(timestamp);
        last = Some(timestamp);
    }

    Ok(match (first, last) {
        (Some(first), Some(last)) => Some(ArchiveCoverage { first, last }),
        _ => None,
    })
}

fn resolve_archive_entry_name<R: io::Read + io::Seek>(
    archive: &mut ZipArchive<R>,
    expected_entry_name: &str,
) -> Result<String> {
    if archive.by_name(expected_entry_name).is_ok() {
        return Ok(expected_entry_name.to_owned());
    }

    let suffix = format!("/{expected_entry_name}");
    let mut matches = Vec::new();
    for i in 0..archive.len() {
        let name = archive
            .by_index(i)
            .with_context(|| format!("failed to inspect ZIP entry #{i}"))?
            .name()
            .to_owned();
        if name.ends_with(&suffix) {
            matches.push(name);
        }
    }

    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => bail!("archive does not contain {expected_entry_name}"),
        _ => bail!(
            "archive contains multiple entries matching {expected_entry_name}: {}",
            matches.join(", ")
        ),
    }
}

fn read_ohlcvt_daily_csv<R: io::Read>(
    reader: &mut csv::Reader<R>,
    entry_name: &str,
    kraken_pair: &str,
    start_ts: i64,
    end_ts: i64,
    target_interval_minutes: u32,
    missing_starts: &mut BTreeSet<i64>,
    cache: &mut HashMap<String, f64>,
) -> Result<CacheWriteStats> {
    let mut stats = CacheWriteStats::default();
    let mut last_logged_year = None;

    for record in reader.records() {
        let record = record.with_context(|| format!("failed to parse a row in {entry_name}"))?;
        let timestamp = parse_archive_timestamp(&record, entry_name)?;
        if timestamp < start_ts || timestamp >= end_ts || !missing_starts.contains(&timestamp) {
            continue;
        }
        log_archive_year_progress(
            timestamp,
            &mut last_logged_year,
            "OHLCVT archive",
            KRAKEN_DAILY_INTERVAL_MINUTES,
        )?;

        let midpoint = parse_archive_midpoint(&record, entry_name)?;
        // The OHLCVT archive lacks VWAP, so we store the UTC daily (open + close) / 2 midpoint
        // under the normal 1440-minute cache key as a simple per-day price proxy.
        stats.record(store_cache_value(
            cache,
            kraken_pair,
            target_interval_minutes,
            timestamp,
            midpoint,
        ));
        missing_starts.remove(&timestamp);
    }

    Ok(stats)
}

fn read_trade_interval_csv<R: io::Read>(
    reader: &mut csv::Reader<R>,
    entry_name: &str,
    kraken_pair: &str,
    start_ts: i64,
    end_ts: i64,
    source_interval_minutes: u32,
    target_interval_minutes: u32,
    missing_starts: &mut BTreeSet<i64>,
    cache: &mut HashMap<String, f64>,
) -> Result<CacheWriteStats> {
    let mut stats = CacheWriteStats::default();
    let interval_seconds = i64::from(source_interval_minutes) * 60;
    let mut current_start = None;
    let mut price_volume_sum = 0.0f64;
    let mut volume_sum = 0.0f64;
    let mut last_logged_year = None;

    for record in reader.records() {
        let record = record.with_context(|| format!("failed to parse a row in {entry_name}"))?;
        let timestamp = parse_archive_timestamp(&record, entry_name)?;
        if timestamp < start_ts {
            continue;
        }
        if timestamp < end_ts {
            log_archive_year_progress(
                timestamp,
                &mut last_logged_year,
                "trade archive",
                source_interval_minutes,
            )?;
        }

        let slot_start = timestamp - timestamp.rem_euclid(interval_seconds);
        if current_start != Some(slot_start) {
            stats.absorb(flush_trade_interval(
                current_start,
                price_volume_sum,
                volume_sum,
                kraken_pair,
                source_interval_minutes,
                target_interval_minutes,
                missing_starts,
                cache,
            )?);
            current_start = Some(slot_start);
            price_volume_sum = 0.0;
            volume_sum = 0.0;
        }

        if timestamp >= end_ts {
            break;
        }
        if !bucket_has_missing_targets(
            missing_starts,
            slot_start,
            source_interval_minutes,
            target_interval_minutes,
        ) {
            continue;
        }

        let price = parse_archive_number(&record, 1, "price", entry_name)?;
        let volume = parse_archive_number(&record, 2, "volume", entry_name)?;
        price_volume_sum += price * volume;
        volume_sum += volume;
    }

    stats.absorb(flush_trade_interval(
        current_start,
        price_volume_sum,
        volume_sum,
        kraken_pair,
        source_interval_minutes,
        target_interval_minutes,
        missing_starts,
        cache,
    )?);

    Ok(stats)
}

fn log_archive_year_progress(
    timestamp: i64,
    last_logged_year: &mut Option<i32>,
    label: &str,
    interval_minutes: u32,
) -> Result<()> {
    let datetime = chrono::DateTime::from_timestamp(timestamp, 0)
        .context("invalid archive progress timestamp")?;
    let year = datetime.year();
    if *last_logged_year == Some(year) {
        return Ok(());
    }

    *last_logged_year = Some(year);
    if interval_minutes == KRAKEN_DAILY_INTERVAL_MINUTES {
        eprintln!("Processing {label} rows for {year}...");
    } else {
        eprintln!(
            "Processing {label} rows for {year} at {}-minute resolution...",
            interval_minutes
        );
    }
    Ok(())
}

fn parse_archive_timestamp(record: &StringRecord, entry_name: &str) -> Result<i64> {
    let value = record
        .get(0)
        .ok_or_else(|| anyhow!("{entry_name} row is missing the timestamp column"))?;
    value
        .parse::<i64>()
        .with_context(|| format!("invalid timestamp {value} in {entry_name}"))
}

fn parse_archive_midpoint(record: &StringRecord, entry_name: &str) -> Result<f64> {
    let open = parse_archive_number(record, 1, "open", entry_name)?;
    let close = parse_archive_number(record, 4, "close", entry_name)?;
    Ok((open + close) / 2.0)
}

fn parse_archive_number(
    record: &StringRecord,
    column: usize,
    label: &str,
    entry_name: &str,
) -> Result<f64> {
    let value = record
        .get(column)
        .ok_or_else(|| anyhow!("{entry_name} row is missing the {label} column"))?;
    value
        .parse::<f64>()
        .with_context(|| format!("invalid {label} value {value} in {entry_name}"))
}

fn fetch_quarterly_archive_files(
    client: &Client,
    folder_id: &str,
    file_prefix: &str,
    archive_label: &str,
) -> Result<HashMap<String, DriveFile>> {
    let url = format!("https://drive.google.com/drive/folders/{folder_id}?usp=sharing");
    let html = client
        .get(&url)
        .send()
        .with_context(|| format!("failed to fetch {url}"))?
        .error_for_status()
        .with_context(|| format!("Google Drive returned an error for {url}"))?
        .text()
        .with_context(|| format!("failed to decode Google Drive folder page at {url}"))?;

    extract_quarterly_drive_files(&html, file_prefix, archive_label)
}

fn extract_quarterly_drive_files(
    html: &str,
    file_prefix: &str,
    archive_label: &str,
) -> Result<HashMap<String, DriveFile>> {
    let blob = extract_drive_ivd_blob(html)
        .context("failed to locate the Google Drive file listing blob")?;
    let mut files = HashMap::new();
    let mut cursor = 0usize;

    while let Some(relative_idx) = blob[cursor..].find(file_prefix) {
        let name_start = cursor + relative_idx;
        let name_end = blob[name_start..]
            .find(".zip")
            .map(|idx| name_start + idx + 4)
            .ok_or_else(|| anyhow!("failed to parse a quarterly {archive_label} archive filename from Google Drive"))?;
        let name = blob[name_start..name_end].to_owned();

        let suffix = &blob[name_end..];
        let prefix = &blob[..name_start];
        let id = extract_id_between(
            suffix,
            "https:\\/\\/drive.google.com\\/file\\/d\\/",
            "\\/view",
        )
        .or_else(|| {
            extract_last_id_between(
                prefix,
                "https:\\/\\/drive.google.com\\/file\\/d\\/",
                "\\/view",
            )
        })
        .with_context(|| format!("failed to find the Google Drive file id for {name}"))?;

        files.insert(name.clone(), DriveFile { id, name });
        cursor = name_end;
    }

    if files.is_empty() {
        bail!("failed to parse quarterly {archive_label} archive files from Google Drive");
    }

    Ok(files)
}

fn extract_drive_ivd_blob(html: &str) -> Option<&str> {
    let start_marker = "window['_DRIVE_ivd'] = '";
    let end_marker = "';if (window['_DRIVE_ivdc'])";
    let start = html.find(start_marker)? + start_marker.len();
    let rest = &html[start..];
    let end = rest.find(end_marker)?;
    Some(&rest[..end])
}

fn extract_id_between(haystack: &str, marker: &str, terminator: &str) -> Option<String> {
    let start = haystack.find(marker)? + marker.len();
    let rest = &haystack[start..];
    let end = rest.find(terminator)?;
    Some(rest[..end].to_owned())
}

fn extract_last_id_between(haystack: &str, marker: &str, terminator: &str) -> Option<String> {
    let start = haystack.rfind(marker)? + marker.len();
    let rest = &haystack[start..];
    let end = rest.find(terminator)?;
    Some(rest[..end].to_owned())
}

fn download_google_drive_file(client: &Client, file_id: &str, destination: &Path) -> Result<()> {
    let initial_url = format!("https://drive.google.com/uc?export=download&id={file_id}&confirm=t");
    let response = client
        .get(&initial_url)
        .send()
        .with_context(|| format!("failed to download Google Drive file {file_id}"))?;

    if is_html_response(&response) {
        let html = response
            .text()
            .context("failed to decode the Google Drive confirmation page")?;
        let action = extract_html_attribute(&html, "<form id=\"download-form\"", "action")
            .context("failed to find the Google Drive download form")?;
        let id = extract_hidden_input_value(&html, "id")
            .context("failed to parse the Google Drive file id")?;
        let export = extract_hidden_input_value(&html, "export")
            .context("failed to parse the Google Drive export mode")?;
        let confirm = extract_hidden_input_value(&html, "confirm")
            .context("failed to parse the Google Drive confirmation token")?;
        let uuid = extract_hidden_input_value(&html, "uuid")
            .context("failed to parse the Google Drive download UUID")?;

        let confirmed = client
            .get(&action)
            .query(&[
                ("id", id),
                ("export", export),
                ("confirm", confirm),
                ("uuid", uuid),
            ])
            .send()
            .with_context(|| format!("failed to confirm the Google Drive download for {file_id}"))?;

        write_response_to_file(confirmed, destination)
    } else {
        write_response_to_file(response, destination)
    }
}

fn ensure_archive_downloaded(
    client: &Client,
    file: &DriveFile,
    destination: &Path,
    archive_mode: ArchiveBackfillMode,
    archive_pair: &str,
    end_ts: i64,
    target_interval_minutes: u32,
) -> Result<PreparedArchive> {
    let mut existing_coverage = None;
    if destination.exists() {
        if file.name == archive_mode.complete_archive_name() {
            let extracted_path =
                ensure_archive_entry_extracted(destination, &file.name, archive_pair, archive_mode)?;
            let coverage = inspect_archive_coverage(
                &extracted_path,
                &format!("cached extracted {archive_pair} data"),
                KRAKEN_DAILY_INTERVAL_MINUTES,
            )?;
            existing_coverage = Some(coverage);
            let needed_start = needed_archive_start(end_ts, target_interval_minutes);
            if coverage.last >= needed_start {
                let size = archive_file_size_label(destination);
                eprintln!(
                    "Reusing cached {} at {}{}.",
                    archive_file_description(file, archive_mode),
                    destination.display(),
                    size.as_deref().unwrap_or(""),
                );
                return Ok(PreparedArchive {
                    archive_path: destination.to_path_buf(),
                    archive_file_name: file.name.clone(),
                    extracted_path,
                });
            }
            eprintln!(
                "Cached {} at {} does not appear to reach the requested {} data. Redownloading...",
                archive_file_description(file, archive_mode),
                destination.display(),
                format_interval_start(needed_start, target_interval_minutes)?,
            );
            clear_temp_cache_dir()?;
        } else {
            let size = archive_file_size_label(destination);
            eprintln!(
                "Reusing cached {} at {}{}.",
                archive_file_description(file, archive_mode),
                destination.display(),
                size.as_deref().unwrap_or(""),
            );
            return Ok(PreparedArchive {
                archive_path: destination.to_path_buf(),
                archive_file_name: file.name.clone(),
                extracted_path: ensure_archive_entry_extracted(
                    destination,
                    &file.name,
                    archive_pair,
                    archive_mode,
                )?,
            });
        }
    }

    let temp_path = temp_download_path(&file.name);
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let result = (|| -> Result<PreparedArchive> {
        let size_hint = archive_file_size_label(destination);
        eprintln!(
            "Downloading {} to {}{}...",
            archive_file_description(file, archive_mode),
            destination.display(),
            size_hint.as_deref().unwrap_or(""),
        );
        download_google_drive_file(client, &file.id, &temp_path)?;
        if file.name == archive_mode.complete_archive_name() {
            let extracted_path =
                ensure_archive_entry_extracted(&temp_path, &file.name, archive_pair, archive_mode)?;
            let downloaded_coverage = inspect_archive_coverage(
                &extracted_path,
                &format!("downloaded extracted {archive_pair} data"),
                KRAKEN_DAILY_INTERVAL_MINUTES,
            )?;
            let needed_start = needed_archive_start(end_ts, target_interval_minutes);
            if downloaded_coverage.last < needed_start {
                bail!(
                    "downloaded complete archive does not reach the requested {} data; keeping existing cached archive at {}",
                    format_interval_start(needed_start, target_interval_minutes)?,
                    destination.display()
                );
            }
            if !accept_complete_archive_replacement(existing_coverage, downloaded_coverage) {
                eprintln!(
                    "Using downloaded complete archive for this run and keeping it at {}, while preserving cached {} at {} because the new archive would shrink prior coverage.",
                    temp_path.display(),
                    archive_mode.archive_label(),
                    destination.display(),
                );
                return Ok(PreparedArchive {
                    archive_path: temp_path.clone(),
                    archive_file_name: file.name.clone(),
                    extracted_path,
                });
            }
        }
        fs::rename(&temp_path, destination).with_context(|| {
            format!(
                "failed to move downloaded archive into cache at {}",
                destination.display()
            )
        })?;
        eprintln!(
            "Saved {} to {}{}.",
            archive_file_description(file, archive_mode),
            destination.display(),
            archive_file_size_label(destination).as_deref().unwrap_or(""),
        );
        let extracted_path =
            ensure_archive_entry_extracted(destination, &file.name, archive_pair, archive_mode)?;
        Ok(PreparedArchive {
            archive_path: destination.to_path_buf(),
            archive_file_name: file.name.clone(),
            extracted_path,
        })
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    result
}

fn inspect_archive_coverage(
    path: &Path,
    label: &str,
    target_interval_minutes: u32,
) -> Result<ArchiveCoverage> {
    eprintln!("Scanning coverage from {} at {}...", label, path.display());
    let coverage = extracted_csv_timestamp_bounds(path, label)?
        .context("archive pair CSV is empty")?;
    eprintln!(
        "{} appears to cover {} through {}.",
        label,
        format_interval_start(coverage.first, target_interval_minutes)?,
        format_interval_start(coverage.last, target_interval_minutes)?,
    );
    Ok(coverage)
}

fn accept_complete_archive_replacement(
    existing_coverage: Option<ArchiveCoverage>,
    downloaded_coverage: ArchiveCoverage,
) -> bool {
    match existing_coverage {
        Some(existing) => {
            downloaded_coverage.first <= existing.first
                && downloaded_coverage.last >= existing.last
        }
        None => true,
    }
}

fn needed_archive_start(end_ts: i64, target_interval_minutes: u32) -> i64 {
    end_ts - i64::from(target_interval_minutes) * 60
}

fn fallback_trade_intervals(target_interval_minutes: u32) -> Vec<u32> {
    KRAKEN_INTERVALS_MINUTES
        .into_iter()
        .filter(|interval| *interval > target_interval_minutes)
        .collect()
}

fn archive_file_description(file: &DriveFile, archive_mode: ArchiveBackfillMode) -> String {
    if file.name == archive_mode.complete_archive_name() {
        format!(
            "Kraken complete {} archive (all years)",
            archive_mode.archive_label()
        )
    } else {
        format!("Kraken {} archive {}", archive_mode.archive_label(), file.name)
    }
}

fn archive_file_size_label(path: &Path) -> Option<String> {
    let bytes = fs::metadata(path).ok()?.len();
    Some(format!(" ({})", format_byte_size(bytes)))
}

fn format_byte_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit_index = 0usize;

    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{value:.1} {}", UNITS[unit_index])
    }
}

fn is_html_response(response: &Response) -> bool {
    response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.starts_with("text/html"))
        .unwrap_or(false)
}

fn write_response_to_file(response: Response, destination: &Path) -> Result<()> {
    let mut response = response.error_for_status()?;
    let mut file = File::create(destination)
        .with_context(|| format!("failed to create {}", destination.display()))?;
    io::copy(&mut response, &mut file)
        .with_context(|| format!("failed to write {}", destination.display()))?;
    Ok(())
}

fn extract_hidden_input_value(html: &str, name: &str) -> Option<String> {
    let marker = format!("name=\"{name}\" value=\"");
    let start = html.find(&marker)? + marker.len();
    let rest = &html[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

fn extract_html_attribute(html: &str, element_prefix: &str, attribute: &str) -> Option<String> {
    let element_start = html.find(element_prefix)?;
    let element = &html[element_start..];
    let marker = format!("{attribute}=\"");
    let attr_start = element.find(&marker)? + marker.len();
    let rest = &element[attr_start..];
    let attr_end = rest.find('"')?;
    Some(rest[..attr_end].to_owned())
}

fn archive_pair_name(kraken_pair: &str) -> Result<String> {
    if let Some(quote) = kraken_pair.strip_prefix("XXBTZ") {
        return Ok(format!("XBT{quote}"));
    }

    if let Some(quote) = kraken_pair.strip_prefix("XXBT") {
        return Ok(format!("XBT{}", quote.trim_start_matches('Z')));
    }

    if kraken_pair.starts_with("XBT") {
        return Ok(kraken_pair.to_owned());
    }

    bail!("cache-rates only supports Kraken XBT quote pairs such as XXBTZUSD or XXBTZEUR")
}

fn flush_trade_interval(
    candle_start: Option<i64>,
    price_volume_sum: f64,
    volume_sum: f64,
    kraken_pair: &str,
    source_interval_minutes: u32,
    target_interval_minutes: u32,
    missing_starts: &mut BTreeSet<i64>,
    cache: &mut HashMap<String, f64>,
) -> Result<CacheWriteStats> {
    let Some(candle_start) = candle_start else {
        return Ok(CacheWriteStats::default());
    };
    let source_interval_seconds = i64::from(source_interval_minutes) * 60;
    let target_interval_seconds = i64::from(target_interval_minutes) * 60;
    let source_end = candle_start + source_interval_seconds;
    let bucket_missing = missing_starts
        .range(candle_start..source_end)
        .copied()
        .collect::<Vec<_>>();
    if bucket_missing.is_empty() {
        return Ok(CacheWriteStats::default());
    }
    if volume_sum <= 0.0 {
        return Ok(CacheWriteStats::default());
    }

    let vwap = price_volume_sum / volume_sum;
    let mut stats = CacheWriteStats::default();
    let mut slot = candle_start;
    while slot < source_end {
        if missing_starts.remove(&slot) {
            stats.record(store_cache_value(
                cache,
                kraken_pair,
                target_interval_minutes,
                slot,
                vwap,
            ));
        }
        slot += target_interval_seconds;
    }
    Ok(stats)
}

fn bucket_has_missing_targets(
    missing_starts: &BTreeSet<i64>,
    candle_start: i64,
    source_interval_minutes: u32,
    target_interval_minutes: u32,
) -> bool {
    let source_interval_seconds = i64::from(source_interval_minutes) * 60;
    let target_interval_seconds = i64::from(target_interval_minutes) * 60;
    let source_end = candle_start + source_interval_seconds;
    let mut slot = candle_start;
    while slot < source_end {
        if missing_starts.contains(&slot) {
            return true;
        }
        slot += target_interval_seconds;
    }
    false
}

fn midnight_utc_timestamp(date: NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0)
        .expect("midnight should always be a valid time")
        .and_utc()
        .timestamp()
}

fn format_interval_start(timestamp: i64, interval_minutes: u32) -> Result<String> {
    let datetime = chrono::DateTime::from_timestamp(timestamp, 0)
        .context("invalid interval start timestamp")?;
    if interval_minutes == KRAKEN_DAILY_INTERVAL_MINUTES {
        Ok(datetime.date_naive().to_string())
    } else {
        Ok(datetime.to_rfc3339())
    }
}

fn archive_download_path(file_name: &str) -> PathBuf {
    PathBuf::from(CACHE_DIR).join("kraken").join(file_name)
}

fn temp_download_path(file_name: &str) -> PathBuf {
    temp_cache_dir().join(format!("download-{}-{}", std::process::id(), file_name))
}

fn extracted_archive_entry_path(archive_file_name: &str, entry_name: &str) -> PathBuf {
    temp_cache_dir().join(format!(
        "{}--{}",
        sanitize_temp_component(archive_file_name),
        sanitize_temp_component(entry_name)
    ))
}

fn temp_extracted_archive_entry_path(archive_file_name: &str, entry_name: &str) -> PathBuf {
    temp_cache_dir().join(format!(
        "extract-{}-{}--{}",
        std::process::id(),
        sanitize_temp_component(archive_file_name),
        sanitize_temp_component(entry_name)
    ))
}

fn temp_cache_dir() -> PathBuf {
    PathBuf::from(CACHE_DIR).join("tmp")
}

fn clear_temp_cache_dir() -> Result<()> {
    let path = temp_cache_dir();
    if path.exists() {
        fs::remove_dir_all(&path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
    }
    fs::create_dir_all(&path).with_context(|| format!("failed to create {}", path.display()))?;
    Ok(())
}

fn sanitize_temp_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

struct TempCacheGuard {
    path: PathBuf,
}

impl TempCacheGuard {
    fn new() -> Result<Self> {
        let path = temp_cache_dir();
        clear_temp_cache_dir()?;
        Ok(Self { path })
    }
}

impl Drop for TempCacheGuard {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = fs::remove_dir_all(&self.path);
            eprintln!("Deleted temporary cache directory {}.", self.path.display());
        }
    }
}

impl ArchiveBackfillMode {
    fn archive_label(self) -> &'static str {
        match self {
            Self::Midpoint => "OHLCVT",
            Self::Vwap => "trade",
        }
    }

    fn quarterly_archive_prefix(self) -> &'static str {
        match self {
            Self::Midpoint => "Kraken_OHLCVT_",
            Self::Vwap => "Kraken_Trading_History_",
        }
    }

    fn quarterly_archive_folder_id(self) -> &'static str {
        match self {
            Self::Midpoint => QUARTERLY_OHLCVT_ARCHIVE_FOLDER_ID,
            Self::Vwap => QUARTERLY_TRADE_ARCHIVE_FOLDER_ID,
        }
    }

    fn complete_archive_file_id(self) -> &'static str {
        match self {
            Self::Midpoint => COMPLETE_OHLCVT_ARCHIVE_FILE_ID,
            Self::Vwap => COMPLETE_TRADE_ARCHIVE_FILE_ID,
        }
    }

    fn complete_archive_name(self) -> &'static str {
        match self {
            Self::Midpoint => "Kraken_OHLCVT.zip",
            Self::Vwap => "Kraken_Trading_History.zip",
        }
    }

    fn entry_name(self, archive_pair: &str) -> String {
        match self {
            Self::Midpoint => format!("{archive_pair}_1440.csv"),
            Self::Vwap => format!("{archive_pair}.csv"),
        }
    }

    fn summary_label(self) -> &'static str {
        match self {
            Self::Midpoint => "OHLCVT archive midpoint rows",
            Self::Vwap => "Trade archive VWAP candles",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::common::AppConfig;

    use super::{
        ArchiveCoverage, CacheRatesArgs, CacheWriteOutcome, DriveFile,
        accept_complete_archive_replacement,
        PreparedArchive, archive_download_path, archive_pair_name, ensure_archive_downloaded,
        extracted_archive_entry_path, extract_quarterly_drive_files, parse_args_from,
        read_ohlcvt_daily_csv, read_trade_interval_csv, resolve_archive_entry_name,
        resolve_quarterly_archive_files, should_store_api_candle, store_cache_value,
        target_interval_minutes,
    };

    const REAL_XBTEUR_1440_SAMPLE: &str = "\
1672531200,15423.8,15524.8,15388.5,15512.9,532.29189029,11775
1672617600,15513.0,15706.6,15455.0,15600.4,1161.60737247,21237
1672704000,15599.1,15892.9,15580.5,15795.1,1465.78622353,24222
";
    const REAL_XBTEUR_1440_OVERLAP_ROW: &str =
        "1711929600,66130.0,66130.0,63461.9,64902.9,528.8081404,23715";
    const REAL_XBTEUR_2026_03_21_TRADES: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/kraken_xbteur_2026-03-21_trades.csv"
    ));
    const REAL_XBTEUR_TRADES_SAMPLE: &str = "\
1735689600,90167.30000,0.00054633
1735689600,90167.30000,0.00005463
1735689600,90167.10000,0.00002185
1735776000,91190.30000,0.00002161
1735776000,91190.30000,0.00001080
1735776000,91190.20000,0.00060000
";

    #[test]
    fn parses_cache_rates_args() {
        let args = parse_args_from(
            vec!["2024".to_owned()],
            "usage: btc_fiat_value cache-rates [--vwap] [--candle <minutes>] <year>",
        )
        .expect("args");
        assert_eq!(
            args,
            CacheRatesArgs {
                year: 2024,
                use_vwap_archive: false,
                candle_override_minutes: None,
            }
        );
    }

    #[test]
    fn parses_cache_rates_vwap_flag() {
        let args = parse_args_from(
            vec!["--vwap".to_owned(), "2024".to_owned()],
            "usage: btc_fiat_value cache-rates [--vwap] [--candle <minutes>] <year>",
        )
        .expect("args");
        assert_eq!(
            args,
            CacheRatesArgs {
                year: 2024,
                use_vwap_archive: true,
                candle_override_minutes: None,
            }
        );
    }

    #[test]
    fn parses_cache_rates_vwap_candle_override() {
        let args = parse_args_from(
            vec![
                "--vwap".to_owned(),
                "--candle".to_owned(),
                "60".to_owned(),
                "2024".to_owned(),
            ],
            "usage: btc_fiat_value cache-rates [--vwap] [--candle <minutes>] <year>",
        )
        .expect("args");
        assert_eq!(
            args,
            CacheRatesArgs {
                year: 2024,
                use_vwap_archive: true,
                candle_override_minutes: Some(60),
            }
        );
    }

    #[test]
    fn rejects_extra_cache_rates_args() {
        let err = parse_args_from(
            vec!["2024".to_owned(), "extra".to_owned()],
            "usage: btc_fiat_value cache-rates [--vwap] [--candle <minutes>] <year>",
        )
        .expect_err("should fail");

        assert!(err
            .to_string()
            .contains("usage: btc_fiat_value cache-rates [--vwap] [--candle <minutes>] <year>"));
    }

    #[test]
    fn rejects_cache_rates_candle_override_without_vwap() {
        let err = parse_args_from(
            vec!["--candle".to_owned(), "60".to_owned(), "2024".to_owned()],
            "usage: btc_fiat_value cache-rates [--vwap] [--candle <minutes>] <year>",
        )
        .expect_err("should fail");

        assert!(err.to_string().contains("--candle requires --vwap"));
    }

    #[test]
    fn vwap_uses_default_candle_minutes_when_present() {
        let config = AppConfig::from_env_values(|name| match name {
            "DEFAULT_CANDLE_MINUTES" => Some("60".to_owned()),
            _ => None,
        })
        .expect("config");

        let interval = target_interval_minutes(
            &CacheRatesArgs {
                year: 2024,
                use_vwap_archive: true,
                candle_override_minutes: None,
            },
            &config,
        )
        .expect("interval");

        assert_eq!(interval, 60);
    }

    #[test]
    fn vwap_candle_override_beats_default_candle_minutes() {
        let config = AppConfig::from_env_values(|name| match name {
            "DEFAULT_CANDLE_MINUTES" => Some("60".to_owned()),
            _ => None,
        })
        .expect("config");

        let interval = target_interval_minutes(
            &CacheRatesArgs {
                year: 2024,
                use_vwap_archive: true,
                candle_override_minutes: Some(240),
            },
            &config,
        )
        .expect("interval");

        assert_eq!(interval, 240);
    }

    #[test]
    fn stores_archives_under_cache_kraken_directory() {
        assert_eq!(
            archive_download_path("Kraken_Trading_History.zip"),
            PathBuf::from(".cache/kraken/Kraken_Trading_History.zip")
        );
    }

    #[test]
    fn midpoint_mode_skips_existing_cache_entries() {
        let mut missing_days = BTreeSet::from([1672531200_i64, 1672617600_i64]);
        let cache = HashMap::from([("XXBTZEUR:1440:1672531200".to_owned(), 12345.0_f64)]);

        super::drop_cached_starts(&mut missing_days, &cache, "XXBTZEUR", 1440);

        assert_eq!(missing_days, BTreeSet::from([1672617600_i64]));
    }

    #[test]
    fn midpoint_mode_skips_api_candles_for_existing_cache_entries() {
        let missing_days = BTreeSet::from([1672617600_i64]);

        assert!(!should_store_api_candle(
            super::ArchiveBackfillMode::Midpoint,
            &missing_days,
            1672531200,
        ));
        assert!(should_store_api_candle(
            super::ArchiveBackfillMode::Midpoint,
            &missing_days,
            1672617600,
        ));
        assert!(should_store_api_candle(
            super::ArchiveBackfillMode::Vwap,
            &missing_days,
            1672531200,
        ));
    }

    #[test]
    fn store_cache_value_skips_unchanged_entries() {
        let mut cache = HashMap::from([("XXBTZEUR:60:1735689600".to_owned(), 107.5_f64)]);

        let outcome = store_cache_value(&mut cache, "XXBTZEUR", 60, 1735689600, 107.5);

        assert_eq!(outcome, CacheWriteOutcome::Skipped);
        assert_eq!(cache["XXBTZEUR:60:1735689600"], 107.5);
    }

    #[test]
    fn store_cache_value_skips_same_cent_drift() {
        let mut cache = HashMap::from([("XXBTZEUR:60:1735689600".to_owned(), 107.504_f64)]);

        let outcome = store_cache_value(&mut cache, "XXBTZEUR", 60, 1735689600, 107.495);

        assert_eq!(outcome, CacheWriteOutcome::Skipped);
        assert_eq!(cache["XXBTZEUR:60:1735689600"], 107.5);
    }

    #[test]
    fn normalizes_xbt_archive_pair_names() {
        assert_eq!(archive_pair_name("XXBTZUSD").unwrap(), "XBTUSD");
        assert_eq!(archive_pair_name("XXBTZEUR").unwrap(), "XBTEUR");
        assert_eq!(archive_pair_name("XBTUSDT").unwrap(), "XBTUSDT");
    }

    #[test]
    fn extracts_quarterly_file_ids_from_drive_html() {
        let html = concat!(
            "window['_DRIVE_ivd'] = '",
            "Kraken_OHLCVT_Q1_2024.zip ",
            "https:\\/\\/drive.google.com\\/file\\/d\\/abc123\\/view ",
            "Kraken_OHLCVT_Q2_2024.zip ",
            "https:\\/\\/drive.google.com\\/file\\/d\\/def456\\/view",
            "';if (window['_DRIVE_ivdc'])"
        );

        let files = extract_quarterly_drive_files(html, "Kraken_OHLCVT_", "OHLCVT")
            .expect("files");
        assert_eq!(files["Kraken_OHLCVT_Q1_2024.zip"].id, "abc123");
        assert_eq!(files["Kraken_OHLCVT_Q2_2024.zip"].id, "def456");
    }

    #[test]
    fn extracts_quarterly_trade_file_ids_from_drive_html() {
        let html = concat!(
            "window['_DRIVE_ivd'] = '",
            "Kraken_Trading_History_Q1_2024.zip ",
            "https:\\/\\/drive.google.com\\/file\\/d\\/abc123\\/view ",
            "Kraken_Trading_History_Q2_2024.zip ",
            "https:\\/\\/drive.google.com\\/file\\/d\\/def456\\/view",
            "';if (window['_DRIVE_ivdc'])"
        );

        let files = extract_quarterly_drive_files(html, "Kraken_Trading_History_", "trade")
            .expect("files");
        assert_eq!(files["Kraken_Trading_History_Q1_2024.zip"].id, "abc123");
        assert_eq!(files["Kraken_Trading_History_Q2_2024.zip"].id, "def456");
    }

    #[test]
    fn falls_back_when_needed_quarterly_trade_archive_is_missing() {
        let quarterly_files = HashMap::from([(
            "Kraken_Trading_History_Q1_2025.zip".to_owned(),
            DriveFile {
                id: "abc123".to_owned(),
                name: "Kraken_Trading_History_Q1_2025.zip".to_owned(),
            },
        )]);
        let needed_quarters = BTreeSet::from([1_u32, 2_u32]);

        let resolved = resolve_quarterly_archive_files(
            &quarterly_files,
            "Kraken_Trading_History_",
            2025,
            &needed_quarters,
        );

        assert!(resolved.is_none());
    }

    #[test]
    fn reads_real_archive_daily_csv_rows_into_cache() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(REAL_XBTEUR_1440_SAMPLE));
        let mut missing_days = BTreeSet::from([1672531200_i64, 1672617600_i64]);
        let mut cache = HashMap::new();

        let stats = read_ohlcvt_daily_csv(
            &mut reader,
            "XBTEUR_1440.csv",
            "XXBTZEUR",
            1672531200,
            1672704000,
            1440,
            &mut missing_days,
            &mut cache,
        )
        .expect("csv should parse");

        assert_eq!(stats.inserted, 2);
        assert_eq!(stats.replaced, 0);
        assert_eq!(stats.skipped, 0);
        assert_eq!(
            cache["XXBTZEUR:1440:1672531200"],
            super::normalize_fiat_rate((15423.8 + 15512.9) / 2.0)
        );
        assert_eq!(
            cache["XXBTZEUR:1440:1672617600"],
            super::normalize_fiat_rate((15513.0 + 15600.4) / 2.0)
        );
        assert!(missing_days.is_empty());
    }

    #[test]
    fn archive_ohlc_matches_live_overlap_day_fields() {
        let record = csv::StringRecord::from(
            REAL_XBTEUR_1440_OVERLAP_ROW
                .split(',')
                .collect::<Vec<_>>(),
        );

        // Sanity check against the live Kraken 1440 OHLC API on 2024-04-01 UTC:
        // the daily candle boundaries and OHLC fields match the archive row.
        assert_eq!(super::parse_archive_timestamp(&record, "XBTEUR_1440.csv").unwrap(), 1711929600);
        assert_eq!(
            super::parse_archive_number(&record, 1, "open", "XBTEUR_1440.csv").unwrap(),
            66130.0
        );
        assert_eq!(
            super::parse_archive_number(&record, 2, "high", "XBTEUR_1440.csv").unwrap(),
            66130.0
        );
        assert_eq!(
            super::parse_archive_number(&record, 3, "low", "XBTEUR_1440.csv").unwrap(),
            63461.9
        );
        assert_eq!(
            super::parse_archive_number(&record, 4, "close", "XBTEUR_1440.csv").unwrap(),
            64902.9
        );
    }

    #[test]
    fn reports_missing_close_column_clearly() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new("1672531200,15423.8,15524.8,15388.5\n"));
        let mut missing_days = BTreeSet::from([1672531200_i64]);
        let mut cache = HashMap::new();

        let err = read_ohlcvt_daily_csv(
            &mut reader,
            "XBTEUR_1440.csv",
            "XXBTZEUR",
            1672531200,
            1672617600,
            1440,
            &mut missing_days,
            &mut cache,
        )
        .expect_err("row should fail");

        assert!(err.to_string().contains("XBTEUR_1440.csv row is missing the close column"));
    }

    #[test]
    fn reports_invalid_close_price_clearly() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new("1672531200,15423.8,15524.8,15388.5,nope,532.29189029,11775\n"));
        let mut missing_days = BTreeSet::from([1672531200_i64]);
        let mut cache = HashMap::new();

        let err = read_ohlcvt_daily_csv(
            &mut reader,
            "XBTEUR_1440.csv",
            "XXBTZEUR",
            1672531200,
            1672617600,
            1440,
            &mut missing_days,
            &mut cache,
        )
        .expect_err("row should fail");

        assert!(err
            .to_string()
            .contains("invalid close value nope in XBTEUR_1440.csv"));
    }

    #[test]
    fn reads_real_trade_csv_rows_into_daily_vwap_cache() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(REAL_XBTEUR_TRADES_SAMPLE));
        let mut missing_days = BTreeSet::from([1735689600_i64, 1735776000_i64]);
        let mut cache = HashMap::new();

        let stats = read_trade_interval_csv(
            &mut reader,
            "XBTEUR.csv",
            "XXBTZEUR",
            1735689600,
            1735862400,
            1440,
            1440,
            &mut missing_days,
            &mut cache,
        )
        .expect("csv should parse");

        let day1 = ((90167.3 * 0.00054633) + (90167.3 * 0.00005463) + (90167.1 * 0.00002185))
            / (0.00054633 + 0.00005463 + 0.00002185);
        let day2 = ((91190.3 * 0.00002161) + (91190.3 * 0.00001080) + (91190.2 * 0.00060000))
            / (0.00002161 + 0.00001080 + 0.00060000);

        assert_eq!(stats.inserted, 2);
        assert_eq!(stats.replaced, 0);
        assert_eq!(stats.skipped, 0);
        assert_eq!(cache["XXBTZEUR:1440:1735689600"], super::normalize_fiat_rate(day1));
        assert_eq!(cache["XXBTZEUR:1440:1735776000"], super::normalize_fiat_rate(day2));
        assert!(missing_days.is_empty());
    }

    #[test]
    fn reads_trade_csv_rows_into_hourly_vwap_cache() {
        let trades = "\
1735689600,100.0,1.0
1735691400,110.0,3.0
1735693200,120.0,2.0
1735695000,90.0,2.0
";
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(trades));
        let mut missing_hours = BTreeSet::from([1735689600_i64, 1735693200_i64]);
        let mut cache = HashMap::new();

        let stats = read_trade_interval_csv(
            &mut reader,
            "XBTEUR.csv",
            "XXBTZEUR",
            1735689600,
            1735696800,
            60,
            60,
            &mut missing_hours,
            &mut cache,
        )
        .expect("csv should parse");

        assert_eq!(stats.inserted, 2);
        assert_eq!(stats.replaced, 0);
        assert_eq!(stats.skipped, 0);
        assert_eq!(cache["XXBTZEUR:60:1735689600"], super::normalize_fiat_rate(107.5));
        assert_eq!(cache["XXBTZEUR:60:1735693200"], super::normalize_fiat_rate(105.0));
        assert!(missing_hours.is_empty());
    }

    #[test]
    fn larger_trade_candle_fills_missing_smaller_intervals() {
        let trades = "\
1735689900,100.0,1.0
1735697100,140.0,3.0
";
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(trades));
        let mut missing_hours =
            BTreeSet::from([1735689600_i64, 1735693200_i64, 1735696800_i64, 1735700400_i64]);
        let mut cache = HashMap::new();

        let stats = read_trade_interval_csv(
            &mut reader,
            "XBTEUR.csv",
            "XXBTZEUR",
            1735689600,
            1735704000,
            240,
            60,
            &mut missing_hours,
            &mut cache,
        )
        .expect("csv should parse");

        let vwap = (100.0 * 1.0 + 140.0 * 3.0) / 4.0;
        assert_eq!(stats.inserted, 4);
        assert_eq!(stats.replaced, 0);
        assert_eq!(stats.skipped, 0);
        assert_eq!(cache["XXBTZEUR:60:1735689600"], super::normalize_fiat_rate(vwap));
        assert_eq!(cache["XXBTZEUR:60:1735693200"], super::normalize_fiat_rate(vwap));
        assert_eq!(cache["XXBTZEUR:60:1735696800"], super::normalize_fiat_rate(vwap));
        assert_eq!(cache["XXBTZEUR:60:1735700400"], super::normalize_fiat_rate(vwap));
        assert!(missing_hours.is_empty());
    }

    #[test]
    fn larger_trade_candle_fills_missing_interior_smaller_intervals() {
        let trades = "\
1735689900,100.0,1.0
1735697100,140.0,3.0
";
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(trades));
        let mut missing_hours = BTreeSet::from([1735693200_i64, 1735696800_i64]);
        let mut cache = HashMap::new();

        let stats = read_trade_interval_csv(
            &mut reader,
            "XBTEUR.csv",
            "XXBTZEUR",
            1735689600,
            1735704000,
            240,
            60,
            &mut missing_hours,
            &mut cache,
        )
        .expect("csv should parse");

        let vwap = (100.0 * 1.0 + 140.0 * 3.0) / 4.0;
        assert_eq!(stats.inserted, 2);
        assert_eq!(stats.replaced, 0);
        assert_eq!(stats.skipped, 0);
        assert_eq!(cache["XXBTZEUR:60:1735693200"], super::normalize_fiat_rate(vwap));
        assert_eq!(cache["XXBTZEUR:60:1735696800"], super::normalize_fiat_rate(vwap));
        assert!(missing_hours.is_empty());
    }

    #[test]
    fn trade_vwap_replaces_existing_cache_entry() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(REAL_XBTEUR_TRADES_SAMPLE));
        let mut missing_days = BTreeSet::from([1735689600_i64]);
        let mut cache = HashMap::from([("XXBTZEUR:1440:1735689600".to_owned(), 1.23_f64)]);

        let stats = read_trade_interval_csv(
            &mut reader,
            "XBTEUR.csv",
            "XXBTZEUR",
            1735689600,
            1735776000,
            1440,
            1440,
            &mut missing_days,
            &mut cache,
        )
        .expect("csv should parse");

        assert_eq!(stats.inserted, 0);
        assert_eq!(stats.replaced, 1);
        assert_eq!(stats.skipped, 0);
        assert_ne!(cache["XXBTZEUR:1440:1735689600"], 1.23);
        assert!(missing_days.is_empty());
    }

    #[test]
    fn trade_vwap_skips_existing_entry_when_value_is_unchanged() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(REAL_XBTEUR_TRADES_SAMPLE));
        let day1 = ((90167.3 * 0.00054633) + (90167.3 * 0.00005463) + (90167.1 * 0.00002185))
            / (0.00054633 + 0.00005463 + 0.00002185);
        let mut missing_days = BTreeSet::from([1735689600_i64]);
        let mut cache = HashMap::from([("XXBTZEUR:1440:1735689600".to_owned(), day1)]);

        let stats = read_trade_interval_csv(
            &mut reader,
            "XBTEUR.csv",
            "XXBTZEUR",
            1735689600,
            1735776000,
            1440,
            1440,
            &mut missing_days,
            &mut cache,
        )
        .expect("csv should parse");

        assert_eq!(stats.inserted, 0);
        assert_eq!(stats.replaced, 0);
        assert_eq!(stats.skipped, 1);
        assert_eq!(cache["XXBTZEUR:1440:1735689600"], super::normalize_fiat_rate(day1));
        assert!(missing_days.is_empty());
    }

    #[test]
    fn trade_archive_fixture_produces_expected_hourly_vwap_candles() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(REAL_XBTEUR_2026_03_21_TRADES));
        // 2026-03-21 02:00–04:59 UTC — three quiet hours from real Kraken trades.
        // API 60-min OHLC VWAPs for this window: 60995.3, 61074.2, 61145.7
        let mut missing_hours =
            BTreeSet::from([1774058400_i64, 1774062000_i64, 1774065600_i64]);
        let mut cache = HashMap::new();

        let stats = read_trade_interval_csv(
            &mut reader,
            "XBTEUR.csv",
            "XXBTZEUR",
            1774058400,
            1774069200,
            60,
            60,
            &mut missing_hours,
            &mut cache,
        )
        .expect("csv should parse");

        assert_eq!(stats.inserted, 3);
        assert_eq!(stats.replaced, 0);
        assert_eq!(stats.skipped, 0);
        assert_eq!(cache["XXBTZEUR:60:1774058400"], super::normalize_fiat_rate(60995.38));
        assert_eq!(cache["XXBTZEUR:60:1774062000"], super::normalize_fiat_rate(61074.23));
        assert_eq!(cache["XXBTZEUR:60:1774065600"], super::normalize_fiat_rate(61145.71));
        assert!(missing_hours.is_empty());
    }

    #[test]
    fn reports_missing_trade_volume_clearly() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new("1735689600,90167.3\n"));
        let mut missing_days = BTreeSet::from([1735689600_i64]);
        let mut cache = HashMap::new();

        let err = read_trade_interval_csv(
            &mut reader,
            "XBTEUR.csv",
            "XXBTZEUR",
            1735689600,
            1735776000,
            1440,
            1440,
            &mut missing_days,
            &mut cache,
        )
        .expect_err("row should fail");

        assert!(err
            .to_string()
            .contains("XBTEUR.csv row is missing the volume column"));
    }

    #[test]
    fn reports_invalid_trade_volume_clearly() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new("1735689600,90167.3,nope\n"));
        let mut missing_days = BTreeSet::from([1735689600_i64]);
        let mut cache = HashMap::new();

        let err = read_trade_interval_csv(
            &mut reader,
            "XBTEUR.csv",
            "XXBTZEUR",
            1735689600,
            1735776000,
            1440,
            1440,
            &mut missing_days,
            &mut cache,
        )
        .expect_err("row should fail");

        assert!(err
            .to_string()
            .contains("invalid volume value nope in XBTEUR.csv"));
    }

    #[test]
    fn reuses_existing_archive_download_without_redownloading() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("btc-accounting-archive-{unique}.zip"));
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        let options: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default();
        writer
            .start_file("XBTEUR.csv", options)
            .expect("start file");
        std::io::Write::write_all(&mut writer, b"1735689600,90167.3,0.1\n").expect("write file");
        let cursor = writer.finish().expect("finish zip");
        std::fs::write(&path, cursor.into_inner()).expect("fixture");
        let extracted_path = extracted_archive_entry_path("unused.zip", "XBTEUR.csv");

        let result = ensure_archive_downloaded(
            &reqwest::blocking::Client::new(),
            &DriveFile {
                id: "unused".to_owned(),
                name: "unused.zip".to_owned(),
            },
            &path,
            super::ArchiveBackfillMode::Vwap,
            "XBTEUR",
            1735776000,
            1440,
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&extracted_path);
        assert_eq!(
            result.expect("result"),
            PreparedArchive {
                archive_path: path,
                archive_file_name: "unused.zip".to_owned(),
                extracted_path,
            }
        );
    }

    #[test]
    fn rejects_complete_archive_replacement_that_drops_old_coverage() {
        let existing = ArchiveCoverage {
            first: 1_672_531_200, // 2023-01-01
            last: 1_735_689_600,  // 2025-01-01
        };
        let downloaded = ArchiveCoverage {
            first: 1_704_067_200, // 2024-01-01
            last: 1_767_225_600,  // 2026-01-01
        };

        assert!(!accept_complete_archive_replacement(
            Some(existing),
            downloaded,
        ));
    }

    #[test]
    fn accepts_complete_archive_for_current_run_when_it_reaches_needed_range() {
        let existing = ArchiveCoverage {
            first: 1_672_531_200, // 2023-01-01
            last: 1_704_067_200,  // 2024-01-01
        };
        let downloaded = ArchiveCoverage {
            first: 1_704_067_200, // 2024-01-01
            last: 1_735_689_600,  // 2025-01-01
        };

        assert!(!accept_complete_archive_replacement(
            Some(existing),
            downloaded,
        ));
        assert!(downloaded.last >= 1_735_689_600);
    }

    #[test]
    fn resolves_wrapped_archive_entry_by_suffix() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        let options: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default();
        writer
            .start_file("TimeAndSales_Combined/XBTEUR.csv", options)
            .expect("start file");
        std::io::Write::write_all(&mut writer, b"1735689600,90167.3,0.1\n").expect("write file");
        let cursor = writer.finish().expect("finish zip");

        let mut archive = zip::ZipArchive::new(Cursor::new(cursor.into_inner())).expect("archive");
        let resolved = resolve_archive_entry_name(&mut archive, "XBTEUR.csv").expect("resolve");

        assert_eq!(resolved, "TimeAndSales_Combined/XBTEUR.csv");
    }
}
