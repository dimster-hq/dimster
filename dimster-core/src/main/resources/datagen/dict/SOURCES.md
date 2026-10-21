# Data generator dictionaries

These files back the dictionary-based string kinds of Dimster's data generator (`first_name`, `city`, `word` and so on). Each file has one value per line, ordered from most to least frequent: the generator maps a sampled rank to an entry by direct index, so with a Zipf distribution the first entries are the most common values, as in real data. Entries are unique within a file, so a dictionary's size is its kind's exact value space.

The generated files are rebuilt with `dev/datagen/build_dictionaries.py` from the sources below; the others are maintained by hand.

| File | Entries | Source | Licence |
|---|---|---|---|
| `last_names.txt` | 5000 | US Census Bureau, 2010 surnames (`Names_2010Census.csv`, https://www2.census.gov/topics/genealogy/2010surnames/names.zip) | Public domain (US government work) |
| `first_names.txt` | 2000 | US Census Bureau, 1990 first names (`dist.female.first`, `dist.male.first`, https://www2.census.gov/topics/genealogy/1990surnames/), merged into one frequency order | Public domain (US government work) |
| `cities.txt` | 1000 | GeoNames `cities15000` (https://download.geonames.org/export/dump/), ASCII names ordered by population | CC BY 4.0, GeoNames (https://www.geonames.org) |
| `countries.txt` | 252 | GeoNames `countryInfo.txt`, ordered by population | CC BY 4.0, GeoNames (https://www.geonames.org) |
| `words.txt` | 3000 | Word frequencies of all-lowercase tokens in Project Gutenberg books 11, 84, 98, 345, 1342, 1661, 1952, 2701 (https://www.gutenberg.org) | The books are public domain in the US; the file is a derived frequency list |
| `street_names.txt`, `street_suffixes.txt` | | Common US street names and suffixes | Hand-written |
| `domains.txt`, `email_domains.txt` | | Popular website and email domains | Hand-written |
| `user_agents.txt` | | Common browser and bot user-agent strings | Hand-written |
