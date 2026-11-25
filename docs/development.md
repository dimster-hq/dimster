# Modifying Dimster itself

This page is for **contributors** — anyone who wants to change Dimster's own source code (the Java engine, the Rust CLI, or the cloud setup CLIs), not just run benchmarks against it.

If you only want to **run** benchmarks, the [getting-started
guide](getting-started/README.md) has everything you need; you don't need
anything on this page.

## Contributing

I see two main paths for working on Dimster code:

1. You have specific requirements and a coding agent. Just hack on Dimster to make it do what you want and leave it at that. No need to contribute to the project with a PR.
2. You think there are one or more features that would be beneficial for the tool. In that case, open an issue to discuss it and if it looks worthwhile while, code it up and submit a PR.

## Who this is for

You'll need this page if you want to:

- Modify the Java benchmark engine in `dimster-core/`.
- Add a feature to the `dimster` CLI in `cli/`.
- Modify the cloud cluster setup helpers in `cloud/` (`cloud/common/`, `cloud/eks-cli/`, `cloud/gke-cli/`).
- Add a new workload mode (an engine), change the deploy logic, or extend the
  Kubernetes manifests.

## Extra prerequisites

On top of the end-user prerequisites in
[01-prerequisites.md](getting-started/01-prerequisites.md):

| Tool        | Purpose                                                         |
|-------------|-----------------------------------------------------------------|
| Java 25+    | Required by the default `./dimster build` path, which runs Maven on the host. Also used for IDE compile + unit tests. |
| Maven (optional) | The repo ships `./mvnw` (the Maven wrapper) so you don't need a system Maven. Install one only if you prefer it. |

If you don't want a host JDK, pass `--in-docker` to `./dimster build`
and Maven will run inside the builder container instead. Both modes
produce identical runtime images.

> **Apple Silicon note.** The host-build path uses your host JDK, so
> just pick an arm64 JDK and you're done — no Rosetta involved.
> `--in-docker` runs Maven against the JDK baked into `dimster-base`,
> which is `linux-amd64` when Colima is `amd64`; that combination
> currently triggers JIT crashes under Rosetta, which is the main
> reason host-build is the default.

## Project layout

```
dimster/
├── cli/                  # The `dimster` CLI (Rust, clap-based)
├── cloud/                # Cloud provisioning workspace (`eks-setup` + `gke-setup` + shared `common`, Rust)
├── dimster-spi/          # Version-agnostic Kafka SPI (interfaces + Capabilities)
├── dimster-kafka-3.x/    # SPI impl bound to kafka-clients 3.x (no share groups)
├── dimster-kafka-4.1/    # SPI impl bound to kafka-clients 4.1 (share consumer only)
├── dimster-kafka-4.2.0/  # SPI impl bound to kafka-clients 4.2.0 (full share-group support)
├── dimster-core/         # Java engine: producers, consumers, coordinator, modes (depends on dimster-spi only)
├── dimster-flink/        # Flink SQL runner jar (Java 17, shaded), submitted to Flink by the coordinator
├── package-common/       # Shared assembly bits for the per-flavor packages
├── package-3.x/          # Per-flavor tarball bundling dimster-core + dimster-kafka-3.x
├── package-4.1/          # ... bundling dimster-core + dimster-kafka-4.1
├── package-4.2.0/        # ... bundling dimster-core + dimster-kafka-4.2.0
├── docker/               # Dockerfile, Dockerfile.base, Maven plugin shim
├── bin/                  # Shell entrypoints copied into the runtime image
├── payload/              # Pre-generated payload files baked into the image
├── charts/               # Python chart-generation tooling
├── run/                  # Workloads, kafka-configs, broker-configs, log4j configs
├── docs/                 # Documentation
└── pom.xml               # Reactor pom (lists every module above)
```

The Maven build is driven by `pom.xml` at the root via the bundled wrapper (`./mvnw`).

## Kafka SPI and per-version impl modules

The Kafka client API surface drifts across versions. Share-group classes (`KafkaShareConsumer`, `AcknowledgeType`, `GroupType.SHARE`, `alterShareGroupOffsets`, ...) only exist in 4.2+, and even point releases sometimes change signatures. To benchmark the full Kafka 3.0+ range from one `dimster-core` source tree, all Kafka-touching code sits behind a small SPI.

**The split:**

- `dimster-spi/` defines version-agnostic interfaces — `KafkaClientFactory`, `RawProducer`, `RawConsumer`, `RawShareConsumer`, `RawAdmin` — plus a `Capabilities(kafkaClientVersion, shareGroupsSupported)` record.
- `dimster-kafka-<flavor>/` provides one factory implementation per supported client version, shipped with a `META-INF/services/dev.dimster.spi.kafka.KafkaClientFactory` resource naming the factory class.
- `dimster-core/` depends only on `dimster-spi` — never on `kafka-clients` directly, and never on any impl module. At startup it resolves the factory via `ServiceLoader.load(KafkaClientFactory.class).findFirst()`.
- `package-<flavor>/` bundles exactly one impl module into the runtime tarball, so the ServiceLoader sees exactly one provider. Mismatch (zero or two) is a startup-time failure with a clear error.

**Capability negotiation.** dimster-core consults `factory.capabilities()` at startup (logging the version + capabilities) and again when workloads are submitted. A share-group workload against a 3.x build fails fast with a "build against 4.2+" message rather than crashing deep inside the SPI with `UnsupportedOperationException`.

**Two non-obvious Maven tricks if you change the SPI:**

- `dimster-spi` pins `kafka-clients` to the *oldest* supported version (3.7.x today — see `kafka.spi.baseline.version` in `dimster-spi/pom.xml`). If someone adds a 4.2-only type to an SPI signature, the SPI module itself fails to compile. That's the early warning that the SPI has accidentally become version-specific.
- At runtime in a per-flavor package, the impl module declares `kafka-clients` at depth 2 of the dep tree (e.g. 4.2.0); the SPI's transitive 3.7.x is at depth 3. Maven's nearest-wins resolution picks the depth-2 jar, so the runtime classpath has exactly one `kafka-clients` — the per-flavor one — while compile-time only sees the 3.7.x stable subset.

**Source-tree gotcha.** `dimster-kafka-<flavor>` depends on `dimster-spi`, **not** on `dimster-core`. The launcher (`bin/benchmark`) splices `dimster-core/target/classes` onto the classpath directly. So `./mvnw -pl dimster-kafka-<flavor> -am compile` will *not* rebuild `dimster-core` after a core edit — stale `.class` files keep winning silently. Use `./mvnw clean compile` (or include `dimster-core` in `-pl`) whenever you change core code.

## CLI dev loop

```bash
cd cli
cargo build --release
cp target/release/dimster ..
cd ..
```

On macOS Apple Silicon, ad-hoc sign the binary after each clean rebuild so
Gatekeeper lets it run:

```bash
codesign --force --sign - ./dimster
```

Iterate by editing files under `cli/src/` and re-running the four lines
above. `cargo build` is incremental, so subsequent rebuilds are seconds.

To run the CLI's own unit tests:

```bash
cd cli && cargo test
```

## Java engine dev loop

### Path A — Host build (default, fast)

```bash
./dimster build local
./dimster deploy local       # only when the deploy spec changed
```

Runs `./mvnw clean install` on the host, then packages the resulting
tarball into the runtime image via `docker/Dockerfile.package`. Reuses
your host `~/.m2` cache, so pure-source rebuilds are quick. Requires
Java 25+ on the host (`java -version` should report 25 or later).

For tighter inner loops — running unit tests, JUnit-debugging in your
IDE — call the Maven wrapper directly without a full image build:

```bash
./mvnw -pl dimster-core test                       # unit tests in the engine module
./mvnw -pl dimster-flink test                      # Flink runner: plans pipelines with the real planner, no Kafka needed
./mvnw -pl dimster-core test -Dtest=SomeTest       # single test
./mvnw clean install -DskipTests                   # full reactor install (no tests)
```

When you're ready to put a change in front of a cluster, run
`./dimster build <env>` to package it.

### Path B — In-Docker build (`--in-docker`, no host JVM)

```bash
./dimster build local --in-docker
./dimster deploy local
```

The multi-stage `docker/Dockerfile` runs `mvnw clean install` inside
the builder container, so no host JVM is needed. Layer caching keeps
pure-source rebuilds in the ~30s range; pom changes invalidate the
dependency layer and take longer.

Use this when you don't have (or don't want to maintain) a host JDK,
or when you need a fully reproducible CI build. On Apple Silicon with
an `amd64` Colima, this path can hit JDK JIT crashes under Rosetta;
prefer the default host-build path there.

> Both paths produce the same runtime image — same `dimster-base`, same
> tarball layout. Always re-run `./dimster build` before claiming a
> Java change works end-to-end.

## Cloud setup CLIs dev loop (`eks-setup` / `gke-setup`)

```bash
cd cloud
cargo build --release
cp target/release/eks-setup target/release/gke-setup ..
cd ..
```

Same shape as the dimster CLI. Run unit tests across all cloud crates with `cargo test` from `cloud/`. macOS users: re-codesign the resulting binaries after each clean rebuild.

## Testing

- **Java engine:** `./mvnw test` from the repo root runs the full reactor's test suite. `./mvnw -pl dimster-core test` scopes to one module.
- **Rust CLIs:** `cargo test` from `cli/` or `cloud/`. Both workspaces ship with unit tests that don't require a cluster.
- **End-to-end:** the only authoritative end-to-end test is to actually
  run a benchmark on a cluster — the [first-benchmark
  page](getting-started/06-first-benchmark.md) walks through it. For
  contributors, the `quick-test` workload (used in that page) is the
  fastest smoke test that exercises the full stack.

## A word on staying compatible

Two design choices the build is sensitive to — please preserve them when
making changes:

- **Both build modes must keep working.** The default host-build path
  shells out to `./mvnw` from `cli/src/commands/build.rs`; the
  `--in-docker` path runs Maven inside the builder container. Both
  produce the same tarball layout and the same runtime image based on
  `dimster-base`. If you change the assembly, the Dockerfiles, or the
  Maven flags, update both paths together.
- **No BuildKit/buildx required.** The Dockerfiles use standard Docker
  layer caching (poms → source) instead of BuildKit cache mounts, so
  they work on legacy Docker daemons without the `buildx` plugin.
