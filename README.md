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

The project requires Java 21 or newer. The initial release is `1.0.0`.

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

Add the runtime artifact and configure both annotation processors explicitly:

```xml
<properties>
    <maven.compiler.release>21</maven.compiler.release>
    <columnar-projection-store.version>1.2.0</columnar-projection-store.version>
    <columnar-projection-store-hardwood.version>1.0.0</columnar-projection-store-hardwood.version>
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

Set an explicit upper bound on the records Hardwood returns per column batch
without constructing column readers yourself:

```java
PriceProjectionStore store =
        PriceProjectionHardwoodLoader.load(reader, 4096);
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

Version `1.1.0` also generates executor-backed overloads for concurrent
destination-column copies:

```java
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

ExecutorService columnCopies = Executors.newFixedThreadPool(3);
try {
    PriceProjectionStore store =
            PriceProjectionHardwoodLoader.load(
                    reader, 4096, columnCopies);
} finally {
    columnCopies.shutdown();
}
```

The executor is borrowed from the caller. The loader and generated store never
shut it down; sealing only releases the store's reference to it. The caller may
reuse one executor across loads and should shut it down only after all such
loads have returned. Omit `4096` to use Hardwood's automatic batch sizing. The
corresponding advanced overload is `load(ColumnReaders, int, Executor)`.

- All `ParquetFileReader` overloads require a non-null reader, create the
  projected `ColumnReaders`, and close only those created column readers. They
  never close the caller's `ParquetFileReader`.
- `load(ParquetFileReader, int)` and
  `load(ParquetFileReader, int, Executor)` treat `batchSize` as the maximum
  records Hardwood returns per column batch. Zero and negative values cause
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
  single-threaded cursor contract. `nextBatch()`, typed column getters, and
  generated batch setters run on the calling thread. For each non-empty batch,
  `append()` submits one independent destination-array copy per projection
  column and waits for every accepted task before advancing the input again.
  Empty input submits no tasks.
- A null executor is rejected before footers are read or caller-owned column
  readers are advanced. If task submission or a copy fails, all accepted copy
  tasks finish before the unchecked failure is propagated and no partial store
  is returned. The input may already have consumed the current batch.
- If the loading thread is interrupted, an executor-backed overload preserves
  its interrupt status and throws `CancellationException` instead of sealing
  and returning a potentially truncated store.
- All column mappings are validated before the first batch is advanced. Every
  Hardwood batch is appended through the generated common-range batch API
  using `[0, recordCount)`; array capacity beyond the logical row count is
  ignored.
- On successful exhaustion, including empty input, the returned store is
  sealed. If reading fails, no partial store is returned, although the input
  may already have been partially consumed.

Generated loaders are stateless. Building a store and consuming
`ColumnReaders` are single-threaded operations under their upstream contracts.
After sealing and safe publication, stores follow Columnar Projection Store's
concurrent-read guarantees.

Concurrent copying is most useful for wide schemas and sufficiently large
batches. It adds one submitted task per column and does not guarantee a speedup
for narrow or memory-bandwidth-bound loads. Reuse an executor rather than
creating one for every small batch. Do not invoke a loader from a task running
on the same saturated bounded executor that the loader borrows: the synchronous
append must be able to run and join its copy tasks. A separate pool also avoids
competing with Hardwood's own decode work.

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

Version `1.0.0` targets exactly:

- Columnar Projection Store `1.2.0`;
- Hardwood core `1.0.0.Final`;
- Java 21 and newer.

Only flat, non-repeated columns are supported. Nested structures, repeated
columns, boxed primitives, column-name remapping, logical-type conversions,
loader-owned filters, writes, and arbitrary projection return types are
outside the `1.0` scope. Accessor names are used verbatim as Hardwood column
names.

Hardwood marks its column-reader and validity APIs experimental. Generated
loaders intentionally compile against those APIs for a zero-row-object bridge;
a future Hardwood release may therefore require a new integration version even
when the Parquet mapping is unchanged.

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

## Development from main

The current `main` branch is version `1.1.0-SNAPSHOT` and requires the
unpublished Hardwood core `1.1.0-SNAPSHOT` and Columnar Projection Store
`1.3.0-SNAPSHOT` to be installed in the local Maven repository. These
development dependencies include indexed, cached footer access for multi-file
readers and the caller-owned executor batch factory. The normal consumer
installation example above remains on the published `1.0.0` integration,
Columnar Projection Store `1.2.0`, and Hardwood `1.0.0.Final`.

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
