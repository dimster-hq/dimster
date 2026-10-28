# Multi-stage build for the Dimster benchmark image.
#
# The builder stage runs the Maven build inside the container so the host
# machine does not need a JVM or Maven to produce the runtime image. It
# builds ONE per-flavor package module — the one named by KAFKA_FLAVOR,
# at the kafka-clients version named by KAFKA_VERSION (passed through as
# -Dkafka.version=...). The runtime stage then unpacks the resulting
# tarball into /benchmark.
#
# Why one flavor per docker invocation rather than all flavors in one:
# -Dkafka.version is a Maven CLI property and overrides the per-module
# default for ALL modules in a single mvnw invocation. So building 3.x
# at 3.7.2 AND 4.2.0 at 4.2.0 in the same builder stage would silently
# mis-version one of them. The Rust CLI loops over needed (flavor, version)
# pairs and runs one `docker build` per pair.
#
# Both stages share `dimster-base:latest` (eclipse-temurin:25-jdk-noble +
# curl) so the JDK used to compile the engine matches the JDK used to run
# it. dimster-base is built separately by `docker/Dockerfile.base` — the
# CLI ensures it exists (and matches the daemon architecture) before
# invoking this Dockerfile.
#
# This file is deliberately compatible with both the legacy Docker
# builder and BuildKit. We don't pre-warm the Maven local repo: most
# users never edit the Java sources, so the one-shot `clean install`
# below is faster end-to-end than a `dependency:go-offline` layer that
# also drags in ~150MB of unused site-plugin transitives.

# ---------------------------------------------------------------------------
# Builder stage: produces the requested per-flavor dimster-*-bin.tar.gz.
# ---------------------------------------------------------------------------
FROM dimster-base:latest AS builder

# Which dimster-kafka-X impl module to build. Must match an existing
# package-X module name suffix — currently 3.x, 4.1, or 4.2.0. No default
# value: the CLI always passes both build-args explicitly. Setting a
# default would make the Docker build log line read
# "ARG KAFKA_FLAVOR=4.2.0" even when an override was passed, which is
# confusing — and a bare `docker build` without --build-arg would
# silently produce the default flavor instead of failing loudly.
ARG KAFKA_FLAVOR
# Concrete kafka-clients version to compile that flavor against. Passed
# to Maven as -Dkafka.version=... so it overrides the per-module default
# in the matching dimster-kafka-X/pom.xml.
ARG KAFKA_VERSION

WORKDIR /src

# Maven wrapper, every module pom, and the source + assets the package
# assembly references (see package-common/src/assemble/bin.xml). All
# module poms are copied even though only one flavor is built per call,
# because Maven's reactor parses the parent pom's <modules> list and
# would error on missing pom files.
COPY mvnw pom.xml ./
COPY .mvn/ .mvn/

# Module poms - copy these first so the layer can be cached even when
# only sources change.
COPY dimster-spi/pom.xml dimster-spi/pom.xml
COPY dimster-core/pom.xml dimster-core/pom.xml
COPY dimster-flink/pom.xml dimster-flink/pom.xml
COPY dimster-kafka-3.x/pom.xml dimster-kafka-3.x/pom.xml
COPY dimster-kafka-4.1/pom.xml dimster-kafka-4.1/pom.xml
COPY dimster-kafka-4.2.0/pom.xml dimster-kafka-4.2.0/pom.xml
COPY package-3.x/pom.xml package-3.x/pom.xml
COPY package-4.1/pom.xml package-4.1/pom.xml
COPY package-4.2.0/pom.xml package-4.2.0/pom.xml
COPY docker/pom.xml docker/pom.xml

# Module sources (the parts that change most often). Sources for unused
# flavors get copied too — Maven only compiles what -pl asks for.
COPY dimster-spi/src/ dimster-spi/src/
COPY dimster-core/src/ dimster-core/src/
COPY dimster-flink/src/ dimster-flink/src/
COPY dimster-kafka-3.x/src/ dimster-kafka-3.x/src/
COPY dimster-kafka-4.1/src/ dimster-kafka-4.1/src/
COPY dimster-kafka-4.2.0/src/ dimster-kafka-4.2.0/src/

# Shared assembly descriptor (referenced by all package-* modules) and
# top-level assets the descriptor pulls into each tarball.
COPY package-common/src/ package-common/src/
COPY bin/ bin/
COPY payload/ payload/
COPY README.md LICENSE ./

# Build only the requested per-flavor package module + its transitive
# deps (-am = also-make), with kafka.version overridden to the requested
# concrete version.
RUN ./mvnw -q -B -ntp -pl package-${KAFKA_FLAVOR} -am clean install \
    -Dlicense.skip=true -DskipTests \
    -Dkafka.version=${KAFKA_VERSION}

# ---------------------------------------------------------------------------
# Runtime stage: unpacks the tarball produced by the builder.
# ---------------------------------------------------------------------------
FROM dimster-base:latest

# kubectl, for workload hook scripts (engineConfig.hooks) that act on the
# cluster. The coordinator never calls it itself. Installed here rather than
# in dimster-base so existing base images pick it up without a rebuild.
ARG KUBECTL_VERSION=v1.31.4
RUN curl -fsSL -o /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/$(dpkg --print-architecture)/kubectl" \
 && chmod +x /usr/local/bin/kubectl

# Re-declare the build-arg so it's visible in this stage (ARGs don't
# cross FROM boundaries). Only KAFKA_FLAVOR is needed in the runtime
# stage — the version is already baked into the tarball produced upstream.
# No default; see builder stage above.
ARG KAFKA_FLAVOR

COPY --from=builder /src/package-${KAFKA_FLAVOR}/target/dimster-*-bin.tar.gz /tmp/dimster.tar.gz

# Unpack to /, then move the version-suffixed dir to a stable /benchmark
# path so downstream tooling and Kubernetes manifests don't depend on the
# Dimster version string OR the kafka flavor suffix.
RUN tar -xzf /tmp/dimster.tar.gz -C / \
 && mv /dimster-* /benchmark \
 && rm /tmp/dimster.tar.gz

WORKDIR /benchmark
