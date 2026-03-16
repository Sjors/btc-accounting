use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use anyhow::{Result, anyhow};
use reqwest::blocking::Client;

use crate::common::{AppConfig, Candle, RateLimitedError, build_http_client, fetch_candle_for_timestamp};

const CACHE_DIR: &str = ".cache";
const CACHE_FILE: &str = "rates.json";
const REQUEST_DELAY: Duration = Duration::from_millis(1500);

/// Provides VWAP exchange rates for a given timestamp.
pub trait ExchangeRateProvider {
    fn get_vwap(&self, timestamp: i64, interval_minutes: u32) -> Result<f64>;
}

/// Fetches rates from the Kraken OHLC API with in-memory + disk cache.
pub struct KrakenProvider {
    tor_client: Option<Client>,
    clearnet_client: Client,
    config: AppConfig,
    cache: Mutex<HashMap<String, f64>>,
    made_request: Mutex<bool>,
    initial_cache_size: usize,
}

impl KrakenProvider {
    pub fn new(config: &AppConfig) -> Result<Self> {
        let tor_client = config
            .kraken_proxy_url()
            .map(|proxy_url| build_http_client("Kraken", Some(proxy_url)))
            .transpose()?;
        let clearnet_client = build_http_client("clearnet Kraken", None)?;

        let disk_cache = Self::load_disk_cache();

        let initial_cache_size = disk_cache.len();

        Ok(Self {
            tor_client,
            clearnet_client,
            config: config.clone(),
            cache: Mutex::new(disk_cache),
            made_request: Mutex::new(false),
            initial_cache_size,
        })
    }

    fn cache_key(pair: &str, interval_minutes: u32, candle_start: i64) -> String {
        format!("{pair}:{interval_minutes}:{candle_start}")
    }

    fn cache_path() -> PathBuf {
        PathBuf::from(CACHE_DIR).join(CACHE_FILE)
    }

    fn load_disk_cache() -> HashMap<String, f64> {
        let path = Self::cache_path();
        fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save_disk_cache(&self) {
        let Ok(cache) = self.cache.lock() else { return };
        let Ok(json) = serde_json::to_string_pretty(&*cache) else { return };
        let _ = fs::create_dir_all(CACHE_DIR);
        let _ = fs::write(Self::cache_path(), json);
    }

    /// Returns true if new entries were written to the disk cache.
    pub fn cache_grew(&self) -> bool {
        self.cache.lock().map(|c| c.len() > self.initial_cache_size).unwrap_or(false)
    }

    fn fetch_candle(&self, timestamp: i64, interval_minutes: u32) -> Result<Candle> {
        // Throttle: sleep before consecutive requests
        {
            let mut made_request = self.made_request.lock().map_err(|e| anyhow!("lock: {e}"))?;
            if *made_request {
                thread::sleep(REQUEST_DELAY);
            }
            *made_request = true;
        }
        let client = self.tor_client.as_ref().unwrap_or(&self.clearnet_client);

        let mut delay = REQUEST_DELAY;
        let max_retries = 5;
        for attempt in 0..=max_retries {
            match fetch_candle_for_timestamp(client, &self.config, timestamp, interval_minutes) {
                Ok(candle) => return Ok(candle),
                Err(e) if attempt < max_retries => {
                    if let Some(rle) = e.downcast_ref::<RateLimitedError>() {
                        delay *= 2;
                        if let Some(server_secs) = rle.retry_after_secs {
                            delay = delay.max(Duration::from_secs(server_secs));
                        }
                        if delay > Duration::from_secs(5) {
                            eprintln!("⏳ Kraken rate limit hit, waiting {}s…", delay.as_secs());
                        }
                        thread::sleep(delay);
                    } else {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }
}

impl ExchangeRateProvider for KrakenProvider {
    fn get_vwap(&self, timestamp: i64, interval_minutes: u32) -> Result<f64> {
        let interval_seconds = i64::from(interval_minutes) * 60;
        let candle_start = (timestamp / interval_seconds) * interval_seconds;
        let key = Self::cache_key(&self.config.kraken_pair, interval_minutes, candle_start);

        {
            let cache = self.cache.lock().map_err(|e| anyhow!("cache lock: {e}"))?;
            if let Some(&vwap) = cache.get(&key) {
                return Ok(vwap);
            }
        }

        let candle = self.fetch_candle(timestamp, interval_minutes)?;
        let vwap = candle.vwap;

        {
            let mut cache = self.cache.lock().map_err(|e| anyhow!("cache lock: {e}"))?;
            cache.insert(key, vwap);
        }
        self.save_disk_cache();

        Ok(vwap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedRateProvider(f64);

    impl ExchangeRateProvider for FixedRateProvider {
        fn get_vwap(&self, _timestamp: i64, _interval_minutes: u32) -> Result<f64> {
            Ok(self.0)
        }
    }

    #[test]
    fn fixed_provider_returns_constant_rate() {
        let provider = FixedRateProvider(50_000.0);
        assert_eq!(provider.get_vwap(1_000_000, 60).unwrap(), 50_000.0);
        assert_eq!(provider.get_vwap(2_000_000, 15).unwrap(), 50_000.0);
    }
}
