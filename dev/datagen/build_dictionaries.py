#!/usr/bin/env python3
"""
Builds the frequency-ordered dictionaries that Dimster's data generator embeds
(dimster-core/src/main/resources/datagen/dict/).

The data generator maps a sampled rank to a dictionary entry by direct index,
so every dictionary must be ordered from most to least frequent: with a Zipf
distribution, rank 0 ("Smith", "James", "the", "Tokyo") is then the most common
value, as it is in real data.

This script is only needed when the dictionaries are rebuilt; the generated
files are checked in. It downloads nothing itself: download the sources listed
in datagen/dict/SOURCES.md into a directory and pass it as the first argument.

Usage:
    python3 -I dev/datagen/build_dictionaries.py <sources-dir> <output-dir>

Expected files in <sources-dir>:
    Names_2010Census.csv                    (US Census 2010 surnames)
    dist.female.first, dist.male.first      (US Census 1990 first names)
    cities15000.txt, countryInfo.txt        (GeoNames)
    gutenberg/*.txt                         (Project Gutenberg plain-text books)
"""

import collections
import csv
import pathlib
import re
import sys

LAST_NAMES = 5000
FIRST_NAMES = 2000
CITIES = 1000
WORDS = 3000


def title(name):
    """SMITH -> Smith, MCDONALD -> Mcdonald (good enough for synthetic data)."""
    return name[:1].upper() + name[1:].lower()


def last_names(src):
    rows = []
    with open(src / "Names_2010Census.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["name"] == "ALL OTHER NAMES":
                continue
            rows.append((int(row["rank"]), title(row["name"])))
    rows.sort()
    return [name for _, name in rows[:LAST_NAMES]]


def first_names(src):
    # The 1990 files give each name's percentage within its sex. Halving them
    # approximates the share of the whole population, so the two lists can be
    # merged into one frequency order.
    freq = collections.Counter()
    for fname in ("dist.female.first", "dist.male.first"):
        with open(src / fname, encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    freq[title(parts[0])] += float(parts[1]) / 2
    return [name for name, _ in freq.most_common(FIRST_NAMES)]


def cities(src):
    rows = []
    with open(src / "cities15000.txt", encoding="utf-8") as f:
        for line in f:
            cols = line.rstrip("\n").split("\t")
            # Column 2 is the ASCII name, column 14 the population.
            rows.append((-int(cols[14] or 0), cols[2]))
    rows.sort()
    seen, out = set(), []
    for _, name in rows:
        if name and name not in seen:
            seen.add(name)
            out.append(name)
        if len(out) == CITIES:
            break
    return out


def countries(src):
    rows = []
    with open(src / "countryInfo.txt", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#"):
                continue
            cols = line.rstrip("\n").split("\t")
            # Column 4 is the country name, column 7 the population. Some
            # GeoNames names carry stray whitespace.
            rows.append((-int(cols[7] or 0), cols[4].strip()))
    rows.sort()
    return [name for _, name in rows if name]


def words(src):
    # Only all-lowercase tokens are counted, which drops proper nouns such as
    # character names ("Elizabeth", "Holmes") that dominate novels but would
    # look odd in generated text.
    freq = collections.Counter()
    for book in sorted((src / "gutenberg").glob("*.txt")):
        text = book.read_text(encoding="utf-8", errors="ignore")
        # Strip the Project Gutenberg licence header and footer.
        start = text.find("*** START OF")
        end = text.find("*** END OF")
        if start != -1 and end != -1:
            text = text[text.find("\n", start):end]
        freq.update(re.findall(r"\b[a-z]{2,}\b", text))
    return [w for w, _ in freq.most_common(WORDS)]


def main():
    src = pathlib.Path(sys.argv[1])
    out = pathlib.Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    for name, values in (
        ("last_names", last_names(src)),
        ("first_names", first_names(src)),
        ("cities", cities(src)),
        ("countries", countries(src)),
        ("words", words(src)),
    ):
        (out / f"{name}.txt").write_text("\n".join(values) + "\n", encoding="utf-8")
        print(f"{name}: {len(values)} entries")


if __name__ == "__main__":
    main()
