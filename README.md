# columnar-projection-store-hardwood

[![Maven Central](https://img.shields.io/maven-central/v/io.github.j-util/columnar-projection-store-hardwood.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.j-util/columnar-projection-store-hardwood)
[![Javadoc](https://javadoc.io/badge2/io.github.j-util/columnar-projection-store-hardwood/javadoc.svg)](https://javadoc.io/doc/io.github.j-util/columnar-projection-store-hardwood)
[![CI](https://github.com/j-util/columnar-projection-store-hardwood/actions/workflows/ci.yml/badge.svg)](https://github.com/j-util/columnar-projection-store-hardwood/actions/workflows/ci.yml)

Generate schema-specific loaders that transfer Hardwood column batches
directly into [Columnar Projection Store](https://github.com/j-util/columnar-projection-store)
without row objects, runtime reflection, dynamic proxies, or per-row
`add()` calls.

This is an independent j-util integration. It is not affiliated with or
endorsed by the Hardwood project or its maintainers.

The project requires Java 21 or newer. Version `1.0.0` is stable;
version `1.1.0-Beta1` is a prerelease.

Published artifacts:

- `columnar-projection-store-hardwood`:
  [Maven Central](https://central.sonatype.com/artifact/io.github.j-util/columnar-projection-store-hardwood)
  and
  [Javadoc](https://javadoc.io/doc/io.github.j-util/columnar-projection-store-hardwood)
- `columnar-projection-store-hardwood-processor`:
  [Maven Central](https://central.sonatype.com/artifact/io.github.j-util/columnar-projection-store-hardwood-processor)
  and
  [Javadoc](https://javadoc.io/doc/io.github.j-util/columnar-projection-store-hardwood-processor)

## Installation

Choose the matching integration and dependency versions:

| Integration | CPS | Hardwood | Status |
| --- | --- | --- | --- |
| `1.0.0` | `1.2.0` | `1.0.0.Final` | Stable |
| `1.1.0-Beta1` | `1.3.0` | `1.1.0.Beta1` | Prerelease |

The Maven configuration below uses the `1.1.0-Beta1` prerelease version set.
Add these sections to your project's `pom.xml` to include the runtime and both
annotation processors:

```xml
<properties>
    <maven.compiler.release>21</maven.compiler.release>
    <columnar-projection-store.version>1.3.0</columnar-projection-store.version>
    <columnar-projection-store-hardwood.version>1.1.0-Beta1</columnar-projection-store-hardwood.version>
</properties>

<dependencies>
    <dependency>
        <groupId>io.github.j-util</groupId>
        <artifactId>columnar-projection-store-hardwood</artifactId>
        <version>${columnar-projection-store-hardwood.version}</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>3.15.0</version>
            <configuration>
                <release>${maven.compiler.release}</release>
                <proc>full</proc>
                <annotationProcessorPaths>
                    <path>
                        <groupId>io.github.j-util</groupId>
                        <artifactId>columnar-projection-store-processor</artifactId>
                        <version>${columnar-projection-store.version}</version>
                    </path>
                    <path>
                        <groupId>io.github.j-util</groupId>
                        <artifactId>columnar-projection-store-hardwood-processor</artifactId>
                        <version>${columnar-projection-store-hardwood.version}</version>
                    </path>
                </annotationProcessorPaths>
            </configuration>
        </plugin>
    </plugins>
</build>
```

For the published stable installation, use the same configuration with
`columnar-projection-store-hardwood.version` set to `1.0.0` and
`columnar-projection-store.version` set to `1.2.0`. That integration brings
Hardwood core `1.0.0.Final` transitively; the prerelease brings `1.1.0.Beta1`.

Both processor paths are required. Their order is immaterial. If the Columnar
Projection Store processor is absent, the Hardwood processor emits a compiler
error explaining which artifact to add.

## Complete example

Define a flat projection whose accessor names match the Parquet columns:

```java
package example;

import io.github.jutil.columnarprojection.ProjectionSchema;
import io.github.jutil.columnarprojection.hardwood.HardwoodProjection;

@ProjectionSchema
@HardwoodProjection
public interface PriceProjection {
    long instrumentId();
    String symbol();
    double price();
}
```

Compilation generates the ordinary Columnar Projection Store types plus
`PriceProjectionHardwoodLoader`. The loader's return type is the actual
collision-safe store interface generated for the schema.

```java
package example;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import java.nio.file.Path;

public final class Prices {
    private Prices() {
    }

    public static PriceProjectionStore read(Path path) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(path))) {
            PriceProjectionStore store =
                    PriceProjectionHardwoodLoader.load(reader);

            PriceProjection first = store.viewAt(0);
            System.out.println(first.symbol() + ": " + first.price());
            return store;
        }
    }
}
```

For a member projection, the generated loader follows the schema's binary
name. `Outer.PriceProjection`, for example, produces the public top-level type
`Outer$PriceProjectionHardwoodLoader`.

## Batch sizing, advanced loading, and ownership

The explicit batch-size and executor overloads below are available in
`1.1.0-Beta1`.

Set an explicit upper bound on the records Hardwood returns per column batch
without constructing column readers yourself:

```java
PriceProjectionStore store =
        PriceProjectionHardwoodLoader.loadWithBatchSize(reader, 4096);
```

The batch size must be greater than zero. It is validated before the loader
reads file footers or constructs column readers. The overloads without a
`batchSize` argument—`load(reader)` and `load(reader, executor)`—continue to
use Hardwood's automatic default or adaptive batch sizing; the integration
does not substitute a hardcoded default.

Use the advanced overload when the caller needs to configure filters, batch
size, or row-group selection:

```java
try (var columns = reader
        .buildColumnReaders(PriceProjectionHardwoodLoader.projection())
        .batchSize(4096)
        .build()) {
    PriceProjectionStore store =
            PriceProjectionHardwoodLoader.load(columns, 100_000);
}
```

Version `1.1.0-Beta1` also generates executor-backed overloads that invoke the
store's ranged per-column appenders concurrently:

```java
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

ExecutorService columnCopies = Executors.newFixedThreadPool(3);
try {
    PriceProjectionStore store =
            PriceProjectionHardwoodLoader.loadWithBatchSize(
                    reader, 4096, columnCopies);
} finally {
    columnCopies.shutdown();
}
```

The executor is borrowed from the caller. The loader never shuts it down, and
the generated store does not retain it. The caller may reuse one executor
across loads and should shut it down only after all such loads have returned.
Use `load(reader, columnCopies)` for Hardwood's automatic batch sizing. The
corresponding advanced overload is `load(ColumnReaders, int, Executor)`.

- All `ParquetFileReader` overloads require a non-null reader, create the
  projected `ColumnReaders`, and close only those created column readers. They
  never close the caller's `ParquetFileReader`.
- `loadWithBatchSize(ParquetFileReader, int)` and
  `loadWithBatchSize(ParquetFileReader, int, Executor)` treat `batchSize` as the
  maximum records Hardwood returns per column batch. Zero and negative values cause
  `IllegalArgumentException` before footers are read, column readers are
  constructed, or input is advanced.
- Before allocating, every `ParquetFileReader` overload reads each supplied
  file's footer in order and uses the exact combined row count as the initial
  capacity. Hardwood
  caches those footers for reuse while materializing every file in the same
  supplied order. A combined total that overflows `long`, or that cannot be
  represented by `int`, causes `ArithmeticException`. An indexed footer-read
  failure is translated from `IOException` to `UncheckedIOException`, with the
  original exception preserved as its cause.
- `load(ColumnReaders, int)` never closes the caller's `ColumnReaders`. It
  consumes them to exhaustion. A negative capacity hint is rejected before
  input is advanced. The explicit capacity remains available for callers that
  configure filters, batches, or row-group selection themselves.
- The executor overloads remain synchronous and preserve Hardwood's
  single-threaded cursor contract. `nextBatch()` and every typed column getter
  run on the calling thread. For each positive batch, the loader captures all
  Hardwood arrays, submits one generated ranged column-appender call per
  projection column using `[0, recordCount)`, and waits for every accepted task
  before advancing the input again. Empty input submits no tasks.
- A null executor is rejected before footers are read or caller-owned column
  readers are advanced. If task submission or a ranged append fails, all
  accepted appender tasks finish before the unchecked failure is propagated
  and no partial store is returned. The input may already have consumed the
  current batch.
- If the loading thread is interrupted, an executor-backed overload preserves
  its interrupt status and throws `CancellationException` instead of sealing
  and returning a potentially truncated store.
- All column mappings are validated before the first batch is advanced.
  Sequential overloads retain the generated common-range `batch(0,
  recordCount)` path. Executor overloads create a separate store through
  `create(expectedSize)` and use only its discovered `columnAppender()` ranged
  methods for positive mutation. Array capacity beyond the logical row count
  is ignored in both paths.
- On successful exhaustion, including empty input, the returned store is
  sealed. If reading fails, no partial store is returned, although the input
  may already have been partially consumed.

Generated loaders are stateless. Building a store and consuming
`ColumnReaders` are single-threaded operations under their upstream contracts.
After sealing and safe publication, stores follow Columnar Projection Store's
concurrent-read guarantees.

Concurrent appending is most useful for wide schemas and sufficiently large
batches. It adds one submitted task per column and does not guarantee a speedup
for narrow or memory-bandwidth-bound loads. Reuse an executor rather than
creating one for every small batch. Do not invoke a loader from a task running
on the same saturated bounded executor that the loader borrows: the synchronous
load must be able to run and join its appender tasks. A separate pool also
avoids competing with Hardwood's own decode work.

## Supported mappings

| Projection return type | Parquet physical type | Hardwood accessor |
| --- | --- | --- |
| `boolean` | `BOOLEAN` | `getBooleans()` |
| `int` | `INT32` | `getInts()` |
| `long` | `INT64` | `getLongs()` |
| `float` | `FLOAT` | `getFloats()` |
| `double` | `DOUBLE` | `getDoubles()` |
| `String` | `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` | `getStrings()` |
| `byte[]` | `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` | `getBinaries()` |

Primitive projection columns must be `REQUIRED`. Optional primitives are
rejected before reading rather than silently translating null to a primitive
default. `String` and `byte[]` columns may be `REQUIRED` or `OPTIONAL`, and
null elements are preserved.

## Limitations and compatibility

Prerelease version `1.1.0-Beta1` targets exactly:

- Columnar Projection Store `1.3.0`;
- Hardwood core `1.1.0.Beta1`;
- Java 21 and newer.

Version `1.0.0` targets exactly:

- Columnar Projection Store `1.2.0`;
- Hardwood core `1.0.0.Final`;
- Java 21 and newer.

Version `1.1.0-Beta1` is intentionally marked prerelease because it depends on
Hardwood's experimental column-reader and validity APIs. Future Hardwood
changes may require a new integration release even when the Parquet mapping
is unchanged. Users wanting the stable dependency line can remain on `1.0.0`.

The prerelease uses indexed, cached footer access for multi-file readers and
the CPS `1.3.0` generated `create(int)`, collision-safe `columnAppender()` return
type, and ranged appender methods required by executor loading.

Only flat, non-repeated columns are supported. Nested structures, repeated
columns, boxed primitives, column-name remapping, logical-type conversions,
loader-owned filters, writes, and arbitrary projection return types are
outside this integration's scope. Accessor names are used verbatim as Hardwood
column names.

Hardwood's codec libraries are optional dependencies. Add the codec required
by the files you read—such as Snappy, Zstandard, LZ4, or Brotli—to the
application. This integration does not choose or add codecs for consumers.

On Java 26, Hardwood `1.0.0.Final` may emit a native-access warning from its
optional libdeflate acceleration; the warning originates in Hardwood, not this
integration library. Hardwood's
[configuration guide](https://hardwood.dev/1.0.0.Final/reference/configuration/)
recommends `--enable-native-access=ALL-UNNAMED` when enabling that acceleration.
Add the option to the application JVM to authorize the native access and avoid
the warning. It is not required merely because the warning appears when
execution otherwise continues.

## Build and tests

```shell
./mvnw clean verify
./mvnw javadoc:aggregate
```

The tests compile clean external-style consumers with both processors in both
orders and generate genuine Parquet files at test time with Apache Parquet
Java's `parquet-hadoop` `1.17.0` example writer. The writer and Hadoop support
are test-scoped and are not present in published POMs.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
