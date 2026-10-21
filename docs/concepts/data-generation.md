# Data Generation

By default Dimster sends payloads as opaque byte arrays that are part random and part zero padding. It includes configuration for "percent randomness" to make compression effectiveness configurable.

However, some current and future use cases may require more realistic data. For those use cases, a workload can set a `dataset`, and producers then send generated rows that look like real data, with the cardinalities, skews and clustering you choose.

This page explains how the generator works. The field reference is in [Workload Files](../reference/workloads.md#generated-datasets); example workloads are in `run/workloads/datagen-test/`.

## Records always have a schema

Every record Dimster produces conceptually has the schema, split into two parts:

1. A `_dimster` byte[] field which encodes fields needed for Dimster functionality, such as the message type, sequence number in correctness mode etc.
2. The remaining fields are the dataset's columns. By default, it is another byte[].

Avro and JSON formats are supported.

## Column recipes

Common column definitions, ready to copy into a dataset's `columns`. All of them are in `run/workloads/datagen-test/datagen-column-recipes.yaml`, a small runnable workload that prints its records (`printContents`), so you can see the values for yourself.

### Numbers

```yaml
score:     {type: INT, min: 0, max: 100}                          # whole number 0-100, every value equally likely
exam_mark: {type: INT, min: 0, max: 100, distribution: NORMAL}    # bell-shaped around 50
price:     {type: DOUBLE, min: 0.5, max: 500, scale: 2}           # money: 0.50-500.00
ratio:     {type: DOUBLE}                                         # fraction in [0, 1)
rating:    {type: INT, min: 1, max: 5, cardinality: 5, distribution: ZIPF, skew: 1.2, scramble: false}  # 1 most common, then 2, ...
```

`min` and `max` are inclusive. Without a `distribution`, values are uniform. `NORMAL` centres on the midpoint with a standard deviation of a sixth of the range, clamping the rare values outside it. `scale` rounds doubles to that many decimal places.

### Identifiers

```yaml
order_id:    {type: SEQUENCE}                                     # unique per record within a topic
customer_id: {type: LONG, min: 1, max: 99999999, cardinality: 10000, distribution: ZIPF}  # 10,000 customers, some far more active
request_id:  {type: STRING, kind: UUID}                           # a fresh UUID on every record
sku:         {type: STRING, kind: ALPHANUMERIC, length: [10, 10], cardinality: 500}       # 500 distinct 10-character codes
```

With a `cardinality`, an integer column draws that many distinct values from `[min, max]`, spread pseudo-randomly through the range (`scramble: false` would make them `min`, `min + 1`, ...).

### A fixed set of values

```yaml
size:       {type: STRING, values: [S, M, L, XL]}                           # equally likely
status:     {type: STRING, values: [OK, RETRY, FAILED], weights: [70, 25, 5]}  # 70% / 25% / 5%
is_premium: {type: BOOLEAN, trueRate: 0.1}                                  # true 10% of the time
```

`values` lists are strings. For a small set of consecutive integers use an `INT` range (`min: 1, max: 5`), optionally with `cardinality` and `ZIPF` for uneven frequencies; arbitrary numeric sets such as {1, 5, 10, 25} are not supported, other than as strings.

### Dates and times

```yaml
event_time:  {type: TIMESTAMP, jitterMs: 5000}                    # now, minus up to 5 s of lateness (epoch millis)
signup_time: {type: LONG, min: 1577836800000, max: 1735603200000} # 2020-01-01 to 2024-12-31 (epoch millis)
birth_day:   {type: INT, min: -7305, max: 13148}                  # 1950-01-01 to 2005-12-31 (days since 1970-01-01)
```

There is no date type: dates are numbers. `TIMESTAMP` is the producer's clock at send time, which suits event times. For other dates, use a `LONG` range of epoch milliseconds or an `INT` range of days since 1970-01-01. In Avro, `TIMESTAMP` columns carry the `timestamp-millis` logical type; the others are plain longs and ints.

### People and places

```yaml
name:    {type: STRING, kind: FULL_NAME, cardinality: 10000}
email:   {type: STRING, kind: EMAIL, cardinality: 10000}
address: {type: STRING, kind: STREET_ADDRESS, cardinality: 10000}
city:    {type: STRING, kind: CITY, distribution: ZIPF, skew: 1.0}   # most-populous cities most often
country: {type: STRING, kind: COUNTRY, cardinality: 20}              # the 20 most-populous countries
```

A dictionary kind with a `cardinality` uses the first (most frequent) entries of its dictionary. Columns are independent, so a row's `email` does not match its `name`.

### Text and web

```yaml
comment:   {type: STRING, kind: SENTENCE, words: [3, 12]}
page:      {type: STRING, kind: URL, cardinality: 1000, distribution: ZIPF}
client_ip: {type: STRING, kind: IP, cardinality: 5000}
```

Sentences are made of common English words with realistic frequencies but no grammar ("To it he them the among the of."), which is what matters for compression.

### Optional and clustered values

```yaml
coupon: {type: STRING, kind: ALPHANUMERIC, length: [8, 8], cardinality: 50, nullRate: 0.8}  # null 80% of the time
region: {type: STRING, values: [north, south, east, west], runLength: 100}                 # changes every 100 records
```

`nullRate` works on any column type. `runLength` holds each value for that many consecutive records on a producer (see [Clustering](#clustering)).

### Example output

Two records from the recipes workload, as printed with `printContents`:

```json
{"_dimster":"00","score":71,"exam_mark":67,"price":161.98,"ratio":0.725509114174186,"rating":2,"order_id":0,"customer_id":94019394,"request_id":"9680fc29-80f2-4f4e-8ee2-cff02a820e3a","sku":"Nnj2wplr4h","size":"S","status":"OK","is_premium":false,"event_time":1792510509701,"signup_time":1698716803088,"birth_day":-3419,"name":"Charles Hammond","email":"alexander.williams14@hotmail.com","address":"4 Willow Street","city":"Shanghai","country":"Democratic Republic of the Congo","comment":"Or he add.","page":"https://www.espn.com/furniture/where/the","client_ip":"36.136.93.242","coupon":null,"region":"north"}
{"_dimster":"00","score":82,"exam_mark":57,"price":476.88,"ratio":0.07756340131900008,"rating":1,"order_id":1,"customer_id":21630995,"request_id":"1f5ec405-f6dd-4280-8aea-2c88c94aeb53","sku":"YezWvSonvA","size":"M","status":"RETRY","is_premium":false,"event_time":1792510512168,"signup_time":1635196697247,"birth_day":16,"name":"Joseph Epperson","email":"louise.cronin@outlook.com","address":"8393 Third Street","city":"Rotterdam","country":"Ethiopia","comment":"To it he them the among the of.","page":"https://www.google.com/enter?id=31945","client_ip":"52.74.13.14","coupon":null,"region":"north"}
```

## How values are generated

Each producer thread has its own generator, seeded from the dataset `seed` and the producer's name, and streams one value per column straight into the record encoder. Per column:

1. With probability `nullRate` the value is null.
2. Otherwise, if the current value has been held for `runLength` rows, a new one is sampled; otherwise the current value repeats.

Sampling depends on the column:

- **Categorical columns** (those with a `cardinality`, a `values` list or a dictionary kind) sample a **rank** in `[0, cardinality)` using the column's `distribution` (uniform or Zipf), then map the rank to a value.
- **Ranges** (numbers without a cardinality) draw a value from `[min, max]`, uniformly or normally, rounded to `scale` decimal places for doubles.
- **Timestamps** are the producer's wall clock minus a random jitter; **sequences** are a per-producer counter.

### Cardinality is global

The rank-to-value mapping is a pure function of the dataset seed, the column name and the rank. It does not depend on the producer, so rank 17 of `customer_name` is the same name on every producer, on every worker and in every run with the same seed. That is what makes `cardinality` the number of distinct values in the whole topic, not per producer.

A column therefore never produces more than `cardinality` distinct values. For integers, distinct ranks always map to distinct values (the mapping into `[min, max]` is a permutation when the domain fits in 31 bits, and a 64-bit hash beyond that). For composite string kinds two ranks can occasionally map to the same value, which is negligible when the kind's value space is much larger than the cardinality; Dimster rejects a cardinality larger than the value space (for example 10,000 distinct `CITY` values from a 1,000-entry dictionary).

### Skew

With `distribution: ZIPF`, rank 0 is the most frequent value and the probability of rank `k` is proportional to `1 / (k + 1)^skew`. For integers, `scramble` (on by default) maps ranks to pseudo-random values, so the hottest customer isn't customer 0; turn it off to make the most frequent values the smallest, which can matter for range filters.

A key column (`keyColumn` with `messageDistributor: KEY_COLUMN`) makes the record key follow a column, so key and partition skew follow that column's distribution.

### Clustering

`runLength` holds each sampled value for that many consecutive rows on a producer. Real data is often clustered like this (events of one session or one region arrive together), and clustering has two big effects: compressors find more repetition within a batch, and batch-level min/max statistics become selective, so filters can skip whole batches. A run length of at least the number of records per producer batch keeps one value per batch.

## Realistic values

String columns can be random alphanumerics, but realistic text compresses very differently from random characters, so the generator ships frequency-ordered dictionaries:

- first and last names from the US Census, cities and countries from GeoNames ordered by population, and English words by frequency in public-domain books (see `dimster-core/src/main/resources/datagen/dict/SOURCES.md`);
- street names, domains, email domains and user agents, written by hand.

Dictionary kinds (`FIRST_NAME`, `CITY`, `WORD`...) map rank `r` to dictionary entry `r`. Because the dictionaries are in frequency order, a Zipf distribution produces realistic frequencies: "Smith" is the most common last name. Composite kinds (`FULL_NAME`, `STREET_ADDRESS`, `EMAIL`, `URL`, `SENTENCE`) build each value from dictionary parts, picking parts with a Zipf skew, using a random generator seeded by the rank, so a rank always yields the same address. `values` with `weights` covers enum-like columns such as event types or order statuses.

Columns are generated independently: in the `orders` preset the customer name does not follow `customer_id`. Cross-column correlations are not modelled.

## Presets

| Preset | Columns | Shape | Avro / JSON bytes per record |
|---|---|---|---|
| `clicks` | 12 | Clickstream events: Zipf users and pages, UUID sessions, user agents, a 35%-null referrer | ~280 / ~460 |
| `orders` | 15 | E-commerce orders: customer names, emails and addresses, a skewed product catalogue, weighted statuses, mostly-null discount codes | ~130 / ~380 |
| `taxi` | 19 | Trips shaped like NYC TLC yellow-taxi records: small integer codes, 265 skewed zones, money amounts with two decimals | ~90 / ~410 |

For a feel of how much the data matters: compressing 64 KB batches of each preset in-process gives these ratios (uncompressed / compressed):

| | zstd (level 3) | lz4 |
|---|---|---|
| clicks, Avro | 3.3 | 2.4 |
| clicks, JSON | 5.4 | 3.4 |
| orders, Avro | 1.9 | 1.5 |
| orders, JSON | 4.8 | 3.2 |
| taxi, Avro | 2.8 | 2.0 |
| taxi, JSON | 10.4 | 6.0 |

### Where presets are defined

Each preset is a YAML file in `dimster-core/src/main/resources/datagen/presets/`, bundled into the Dimster jar:

| Preset | File |
|---|---|
| `clicks` | `dimster-core/src/main/resources/datagen/presets/clicks.yaml` |
| `orders` | `dimster-core/src/main/resources/datagen/presets/orders.yaml` |
| `taxi` | `dimster-core/src/main/resources/datagen/presets/taxi.yaml` |

A preset file holds only a `columns:` map, in the same syntax as a workload's `dataset.columns`, so the files double as examples. When reviewing a workload that uses a preset, open its file to see the columns the workload starts from. The records then have:

1. the preset's columns, in file order, with any inline column of the same name overriding it field by field (so sweeping one property needs only that property);
2. then any inline columns the preset doesn't have, in workload order.

Overrides can change but not remove a preset column; see [Overriding Preset Columns](../reference/workloads.md#overriding-preset-columns) for an example and the details.

To see the result rather than work it out, run the workload with `engineConfig.printContents: true` at a low rate (see [Seeing the records](#seeing-the-records)).

To add a preset, add `<name>.yaml` to that directory (names are lowercase letters, digits, `_` and `-`) and rebuild the image. The default `payload` dataset, used when a workload sets no `dataset`, is not a preset file: its one column depends on `messageSize` and the payload settings, so Dimster builds it in code (`DatasetResolver.payloadDataset`).

## Sizes and results

Record sizes follow from the data, so `messageSize` does not apply to a dataset. When a test point is expanded, Dimster generates a sample of rows and uses their average encoded size as the test point's `messageSize`. Everything that converts message counts to bytes (drain-backlog sizes, explore MB/s, the `messageSize` in results) therefore stays approximately right, and charts show it as `~272 (clicks/AVRO)`.

For exact figures, run, correctness and drain-backlog results include measured byte rates: `publishBytesRate` and `consumeBytesRate` per period (MB/s, 10^6 bytes) and `avgRecordBytes`. Explore results report `peakSustainableThroughputMBs` from the estimated size. Charts and `dimster csv` use the measured rates when present, and fall back to rate × messageSize for older results.

## Seeing the records

To see what a dataset actually produces, set `engineConfig.printContents: true` and keep the producer rate at 50 msg/s or less. Workers then log each produced and consumed record with its key and contents, with Avro records decoded to the same JSON form as the JSON format:

```
PRODUCED topic=dimster-92nYCAk-0000 partition=0 offset=1 timestamp=1792509128577 key=9180215 value={"_dimster":"00","event_id":1,"event_time":1792509126315,"user_id":9180215,"session_id":"c39a525f-5630-4603-b52c-c1d124e7fe59","event_type":"page_view","url":"https://www.indeed.com/help","referrer":null,"user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36","ip_address":"190.140.35.158","country":"Morocco","device":"mobile","duration_ms":30012}
```

## Performance

A producer thread generates and encodes roughly 1 to 4 million rows per second, depending on the preset and format, so generation is not the bottleneck at realistic per-producer rates. String values of columns with up to 100,000 distinct values are cached once per worker and shared by its producer threads.

## Limitations

- There is no date type (dates are epoch-based numbers), and `values` sets are strings only.
- `NORMAL` always centres on the midpoint of `[min, max]`; the mean and spread can't be set separately.
- Columns are independent; there are no cross-column correlations.
- Timestamps follow the producer's wall clock, so rows are reproducible only for datasets without `TIMESTAMP` columns.
- `SEQUENCE` values are unique within a topic, not across topics.
- `dataset` can't be changed with `dimster run --modify`.
