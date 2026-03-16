#!/usr/bin/env python3
"""Compare XML elements and attributes used in two CAMT.053 files."""

import sys
import xml.etree.ElementTree as ET
from collections import Counter


def collect_fields(path: str) -> tuple[set[str], Counter]:
    tree = ET.parse(path)
    root = tree.getroot()

    tags = set()
    attr_combos: Counter = Counter()

    def strip_ns(tag: str) -> str:
        return tag.split("}")[1] if "}" in tag else tag

    def walk(el: ET.Element, parent_path: str) -> None:
        tag = strip_ns(el.tag)
        current_path = f"{parent_path}/{tag}" if parent_path else tag
        tags.add(current_path)

        for attr, val in el.attrib.items():
            attr_combos[f"{current_path}[@{attr}={val!r}]"] += 1

        for child in el:
            walk(child, current_path)

    walk(root, "")
    return tags, attr_combos


def main() -> None:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <good.xml> <bad.xml>")
        sys.exit(1)

    good_path, bad_path = sys.argv[1], sys.argv[2]

    good_tags, good_attrs = collect_fields(good_path)
    bad_tags, bad_attrs = collect_fields(bad_path)

    only_in_good = sorted(good_tags - bad_tags)
    only_in_bad = sorted(bad_tags - good_tags)

    only_in_good_attrs = sorted(set(good_attrs) - set(bad_attrs))
    only_in_bad_attrs = sorted(set(bad_attrs) - set(good_attrs))

    if only_in_good:
        print(f"=== Elements only in GOOD ({good_path}) ===")
        for t in only_in_good:
            print(f"  {t}")
    else:
        print("=== No elements unique to GOOD ===")

    print()

    if only_in_bad:
        print(f"=== Elements only in BAD ({bad_path}) ===")
        for t in only_in_bad:
            print(f"  {t}")
    else:
        print("=== No elements unique to BAD ===")

    print()

    if only_in_good_attrs:
        print(f"=== Attribute values only in GOOD ===")
        for a in only_in_good_attrs:
            print(f"  {a}")
    else:
        print("=== No attribute values unique to GOOD ===")

    print()

    if only_in_bad_attrs:
        print(f"=== Attribute values only in BAD ===")
        for a in only_in_bad_attrs:
            print(f"  {a}")
    else:
        print("=== No attribute values unique to BAD ===")

    print()
    print(f"Common elements: {len(good_tags & bad_tags)}")


if __name__ == "__main__":
    main()
