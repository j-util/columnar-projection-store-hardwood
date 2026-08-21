package io.github.jutil.columnarprojection.hardwood.processor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import io.github.jutil.columnarprojection.ProjectionStore;
import io.github.jutil.columnarprojection.processor.ProjectionSchemaProcessor;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@SuppressWarnings({"deprecation", "rawtypes", "unchecked"})
class HardwoodLoaderIntegrationTest {

    private static final String ALL_MAPPINGS_SCHEMA = """
            message test {
              required boolean flag;
              required int32 count;
              required int64 total;
              required float ratio;
              required double price;
              optional binary text (UTF8);
              optional binary blob;
              required fixed_len_byte_array(4) fixedText;
              optional fixed_len_byte_array(3) fixedBlob;
            }
            """;

    @TempDir
    Path temporaryDirectory;

    private URLClassLoader generatedClassLoader;

    @BeforeEach
    void compileConsumer() throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("example.AllMappings", """
                package example;

                @io.github.jutil.columnarprojection.ProjectionSchema
                @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                public interface AllMappings {
                    boolean flag();
                    int count();
                    long total();
                    float ratio();
                    double price();
                    String text();
                    byte[] blob();
                    String fixedText();
                    byte[] fixedBlob();
                }
                """);
        sources.put("example.IntProjection", """
                package example;

                @io.github.jutil.columnarprojection.ProjectionSchema
                @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                public interface IntProjection {
                    int value();
                }
                """);
        sources.put("example.Consumer", """
                package example;

                final class Consumer {
                    static AllMappingsStore load(
                            dev.hardwood.reader.ParquetFileReader reader) {
                        return AllMappingsHardwoodLoader.load(reader);
                    }

                    static IntProjectionStore loadInt(
                            dev.hardwood.reader.ColumnReaders readers) {
                        return IntProjectionHardwoodLoader.load(readers, 0);
                    }
                }
                """);

        CompilerTestSupport.Compilation compilation =
                CompilerTestSupport.compile(
                        temporaryDirectory.resolve("consumer"),
                        sources,
                        List.of(
                                new HardwoodProjectionProcessor(),
                                new ProjectionSchemaProcessor()));
        assertTrue(compilation.successful(), compilation.messages());
        String generated = compilation.generatedSource(
                "example.AllMappingsHardwoodLoader");
        assertTrue(generated.contains("store.batch(0, recordCount)"),
                generated);
        assertTrue(generated.contains("store.columnAppender()"), generated);
        assertFalse(generated.contains("create(expectedSize, executor)"),
                generated);
        assertTrue(generated.contains(".getBooleans()"), generated);
        assertTrue(generated.contains(".getInts()"), generated);
        assertTrue(generated.contains(".getLongs()"), generated);
        assertTrue(generated.contains(".getFloats()"), generated);
        assertTrue(generated.contains(".getDoubles()"), generated);
        assertTrue(generated.contains(".getStrings()"), generated);
        assertTrue(generated.contains(".getBinaries()"), generated);
        generatedClassLoader = compilation.classLoader();
    }

    @AfterEach
    void closeClassLoader() throws IOException {
        if (generatedClassLoader != null) {
            generatedClassLoader.close();
        }
    }

    @Test
    void bridgesEveryPhysicalMappingAcrossMultipleBatchesInRowOrder()
            throws Exception {
        Path parquet = temporaryDirectory.resolve("all-mappings.parquet");
        writeAllMappings(parquet, 5);

        ProjectionStore<?> store = loadAdvanced(
                "example.AllMappingsHardwoodLoader", parquet, 2, 0);

        assertEquals(5, store.size());
        assertNotNull(store.cursor(), "a cursor proves the store is sealed");
        @SuppressWarnings({"rawtypes", "unchecked"})
        ProjectionStore rawStore = store;
        assertThrows(IllegalStateException.class, () -> rawStore.add(null));

        Class<?> projection = generatedClassLoader.loadClass(
                "example.AllMappings");
        for (int row = 0; row < 5; row++) {
            Object view = store.viewAt(row);
            assertEquals(row % 2 == 0,
                    invoke(projection, view, "flag"));
            assertEquals(10 + row, invoke(projection, view, "count"));
            assertEquals(1000L + row, invoke(projection, view, "total"));
            assertEquals(1.5F + row, invoke(projection, view, "ratio"));
            assertEquals(20.25D + row, invoke(projection, view, "price"));
            assertEquals(row == 1 || row == 4 ? null : "text-" + row,
                    invoke(projection, view, "text"));
            byte[] expectedBlob = row == 2
                    ? null
                    : new byte[]{(byte) row, (byte) (row + 1)};
            assertArrayEquals(expectedBlob,
                    (byte[]) invoke(projection, view, "blob"));
            assertEquals("F00" + row,
                    invoke(projection, view, "fixedText"));
            byte[] expectedFixed = row == 3
                    ? null
                    : new byte[]{7, 8, (byte) row};
            assertArrayEquals(expectedFixed,
                    (byte[]) invoke(projection, view, "fixedBlob"));
        }
    }

    @Test
    void executorLoadSubmitsOneCopyPerColumnPerPositiveBatch()
            throws Exception {
        Path parquet = temporaryDirectory.resolve("executor-batches.parquet");
        writeAllMappings(parquet, 5);
        CountingDirectExecutor executor = new CountingDirectExecutor();

        ProjectionStore<?> store = loadAdvanced(
                "example.AllMappingsHardwoodLoader",
                parquet,
                2,
                0,
                executor);

        assertEquals(27, executor.submissionCount,
                "nine columns across three positive batches");
        assertEquals(executor.submissionCount, executor.completionCount,
                "load must not return before copy tasks complete");
        assertEquals(5, store.size());
        assertNotNull(store.cursor());
    }

    @Test
    void executorLoadSubmitsNoTasksForEmptyInput() throws Exception {
        Path parquet = temporaryDirectory.resolve("executor-empty.parquet");
        writeAllMappings(parquet, 0);
        CountingDirectExecutor executor = new CountingDirectExecutor();

        ProjectionStore<?> store = loadAdvanced(
                "example.AllMappingsHardwoodLoader",
                parquet,
                2,
                0,
                executor);

        assertEquals(0, executor.submissionCount);
        assertEquals(0, store.size());
        assertNotNull(store.cursor());
    }

    @Test
    void parquetBatchSizeOverloadLoadsMultipleBatchesInOrderAndKeepsReaderOpen()
            throws Exception {
        Path parquet = temporaryDirectory.resolve("explicit-batches.parquet");
        writeInts(parquet, 60, 61, 62, 63, 64);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        Method load = loader.getMethod(
                "load", ParquetFileReader.class, int.class);
        ColumnProjection projection = (ColumnProjection) loader
                .getMethod("projection")
                .invoke(null);

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(parquet))) {
            ProjectionStore<?> store = (ProjectionStore<?>) load.invoke(
                    null, reader, 2);

            assertIntValues(store, 60, 61, 62, 63, 64);
            assertNotNull(store.cursor());
            assertEquals(5, capacity(store));
            try (ColumnReaders secondRead = reader
                    .buildColumnReaders(projection)
                    .batchSize(1)
                    .build()) {
                assertTrue(secondRead.nextBatch(),
                        "the explicit-batch overload must not close reader");
            }
        }
    }

    @Test
    void parquetBatchSizeExecutorOverloadUsesEveryBatchAndPreservesOrder()
            throws Exception {
        Path parquet = temporaryDirectory.resolve(
                "explicit-executor-batches.parquet");
        writeInts(parquet, 70, 71, 72, 73, 74);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        Method load = loader.getMethod(
                "load",
                ParquetFileReader.class,
                int.class,
                Executor.class);
        ColumnProjection projection = (ColumnProjection) loader
                .getMethod("projection")
                .invoke(null);
        CountingDirectExecutor executor = new CountingDirectExecutor();

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(parquet))) {
            ProjectionStore<?> store = (ProjectionStore<?>) load.invoke(
                    null, reader, 2, executor);

            assertEquals(3, executor.submissionCount,
                    "one column across three positive batches");
            assertEquals(executor.submissionCount, executor.completionCount,
                    "the synchronous load must await every copy");
            assertIntValues(store, 70, 71, 72, 73, 74);
            assertNotNull(store.cursor());
            try (ColumnReaders secondRead = reader
                    .buildColumnReaders(projection)
                    .batchSize(1)
                    .build()) {
                assertTrue(secondRead.nextBatch(),
                        "the executor overload must not close reader");
            }
        }

        FutureTask<String> stillUsable = new FutureTask<>(() -> "still usable");
        executor.execute(stillUsable);
        assertEquals("still usable", stillUsable.get(),
                "the loader must not own the executor");
    }

    @Test
    void parquetBatchSizeOverloadsReturnSealedStoresForEmptyInput()
            throws Exception {
        Path parquet = temporaryDirectory.resolve(
                "explicit-empty.parquet");
        writeInts(parquet);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        Method sequentialLoad = loader.getMethod(
                "load", ParquetFileReader.class, int.class);
        Method executorLoad = loader.getMethod(
                "load",
                ParquetFileReader.class,
                int.class,
                Executor.class);

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(parquet))) {
            ProjectionStore<?> store = (ProjectionStore<?>) sequentialLoad
                    .invoke(null, reader, 2);
            assertEquals(0, store.size());
            assertNotNull(store.cursor());
        }

        CountingDirectExecutor executor = new CountingDirectExecutor();
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(parquet))) {
            ProjectionStore<?> store = (ProjectionStore<?>) executorLoad
                    .invoke(null, reader, 2, executor);
            assertEquals(0, store.size());
            assertNotNull(store.cursor());
        }
        assertEquals(0, executor.submissionCount,
                "empty input must not submit destination copies");
    }

    @Test
    void invalidParquetBatchSizesAreRejectedBeforeInputWork()
            throws Exception {
        Path parquet = temporaryDirectory.resolve(
                "invalid-explicit-batch.parquet");
        writeInts(parquet, 80, 81, 82);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        Method sequentialLoad = loader.getMethod(
                "load", ParquetFileReader.class, int.class);
        Method executorLoad = loader.getMethod(
                "load",
                ParquetFileReader.class,
                int.class,
                Executor.class);
        CountingDirectExecutor executor = new CountingDirectExecutor();

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(parquet))) {
            for (int invalidBatchSize : new int[]{0, -1}) {
                IllegalArgumentException sequentialFailure =
                        assertInvocationFailure(
                                IllegalArgumentException.class,
                                sequentialLoad,
                                reader,
                                invalidBatchSize);
                assertTrue(sequentialFailure.getMessage().contains(
                        Integer.toString(invalidBatchSize)));
                assertInvocationFailure(
                        IllegalArgumentException.class,
                        executorLoad,
                        reader,
                        invalidBatchSize,
                        executor);
            }

            assertEquals(0, executor.submissionCount);
            ProjectionStore<?> store = (ProjectionStore<?>) sequentialLoad
                    .invoke(null, reader, 1);
            assertIntValues(store, 80, 81, 82);
        }

        Path first = temporaryDirectory.resolve(
                "invalid-batch-footer-first.parquet");
        Path second = temporaryDirectory.resolve(
                "invalid-batch-footer-second.parquet");
        writeInts(first, 90);
        writeInts(second, 91);
        for (int invalidBatchSize : new int[]{0, -2}) {
            IOException original = new IOException(
                    "footer trap for " + invalidBatchSize);
            InputFile failing = new MetadataFailingInputFile(
                    InputFile.of(second), original);
            try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(
                    InputFile.of(first), failing))) {
                assertInvocationFailure(
                        IllegalArgumentException.class,
                        sequentialLoad,
                        reader,
                        invalidBatchSize);
                assertInvocationFailure(
                        IllegalArgumentException.class,
                        executorLoad,
                        reader,
                        invalidBatchSize,
                        executor);

                UncheckedIOException footerFailure = assertInvocationFailure(
                        UncheckedIOException.class,
                        sequentialLoad,
                        reader,
                        1);
                assertSame(original, footerFailure.getCause(),
                        "the invalid calls must precede the trapped footer");
            }
        }
    }

    @Test
    void parquetExecutorOverloadBorrowsExecutor() throws Exception {
        Path parquet = temporaryDirectory.resolve("borrowed-executor.parquet");
        writeInts(parquet, 70, 71, 72);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Class<?> loader = generatedClassLoader.loadClass(
                    "example.IntProjectionHardwoodLoader");
            ProjectionStore<?> store;
            try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(parquet))) {
                store = (ProjectionStore<?>) loader
                        .getMethod(
                                "load",
                                ParquetFileReader.class,
                                Executor.class)
                        .invoke(null, reader, executor);
            }

            assertIntValues(store, 70, 71, 72);
            assertFalse(executor.isShutdown(),
                    "the generated loader and sealed store borrow executor");
            assertEquals("still usable",
                    executor.submit(() -> "still usable").get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void interruptDuringNonFinalCopyCancelsBeforeAdvancingNextBatch()
            throws Exception {
        Path parquet = temporaryDirectory.resolve("interrupted-load.parquet");
        writeInts(parquet, 90, 91, 92);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        ColumnProjection projection = (ColumnProjection) loader
                .getMethod("projection")
                .invoke(null);

        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(parquet));
             ColumnReaders columns = reader.buildColumnReaders(projection)
                    .batchSize(1)
                    .build()) {
            Method load = loader.getMethod(
                    "load", ColumnReaders.class, int.class, Executor.class);
            InterruptingFirstTaskExecutor executor =
                    new InterruptingFirstTaskExecutor();
            AtomicReference<ProjectionStore<?>> result = new AtomicReference<>();
            AtomicReference<Throwable> loadFailure = new AtomicReference<>();
            AtomicBoolean interruptedAfterLoad = new AtomicBoolean();
            AtomicBoolean nextBatchAvailable = new AtomicBoolean();
            AtomicReference<Integer> nextValue = new AtomicReference<>();
            AtomicReference<Throwable> inspectionFailure =
                    new AtomicReference<>();
            CountDownLatch loadFinished = new CountDownLatch(1);
            Thread loadingThread = new Thread(() -> {
                try {
                    result.set((ProjectionStore<?>) load.invoke(
                            null, columns, 0, executor));
                } catch (InvocationTargetException exception) {
                    loadFailure.set(exception.getCause());
                } catch (Throwable failure) {
                    loadFailure.set(failure);
                } finally {
                    boolean interrupted = Thread.currentThread()
                            .isInterrupted();
                    interruptedAfterLoad.set(interrupted);
                    if (interrupted) {
                        Thread.interrupted();
                    }
                    try {
                        boolean available = columns.nextBatch();
                        nextBatchAvailable.set(available);
                        if (available) {
                            nextValue.set(columns.getColumnReader("value")
                                    .getInts()[0]);
                        }
                    } catch (Throwable failure) {
                        inspectionFailure.set(failure);
                    } finally {
                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }
                        loadFinished.countDown();
                    }
                }
            }, "hardwood-interrupted-load-caller");
            loadingThread.setDaemon(true);
            executor.setLoadingThread(loadingThread);

            try {
                loadingThread.start();
                assertTrue(executor.awaitInterrupt(5, TimeUnit.SECONDS));
                assertEquals(1L, loadFinished.getCount(),
                        "append must quiesce its accepted copy before failing");

                executor.releaseFirstTask();
                assertTrue(loadFinished.await(5, TimeUnit.SECONDS));

                assertNull(result.get(),
                        "an interrupted load must not return a truncated store");
                assertInstanceOf(CancellationException.class,
                        loadFailure.get());
                assertTrue(interruptedAfterLoad.get());
                assertEquals(1, executor.submissionCount(),
                        "the interrupted load must not append another batch");
                assertNull(inspectionFailure.get());
                assertTrue(nextBatchAvailable.get(),
                        "the second input batch must remain unadvanced");
                assertEquals(91, nextValue.get());

                FutureTask<String> stillUsable = new FutureTask<>(
                        () -> "still usable");
                executor.execute(stillUsable);
                assertEquals("still usable", stillUsable.get());
            } finally {
                executor.releaseFirstTask();
                loadingThread.join(TimeUnit.SECONDS.toMillis(5));
            }
            assertFalse(loadingThread.isAlive());
        }
    }

    @Test
    void rejectedAppenderTaskWaitsForAcceptedWorkAndBorrowsExecutor()
            throws Exception {
        Path parquet = temporaryDirectory.resolve("rejected-copy.parquet");
        writeAllMappings(parquet, 1);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.AllMappingsHardwoodLoader");
        ColumnProjection projection = (ColumnProjection) loader
                .getMethod("projection")
                .invoke(null);
        RejectAfterAcceptedTaskExecutor executor =
                new RejectAfterAcceptedTaskExecutor();

        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(parquet));
             ColumnReaders columns = reader.buildColumnReaders(projection)
                    .batchSize(1)
                    .build()) {
            Method load = loader.getMethod(
                    "load", ColumnReaders.class, int.class, Executor.class);
            AtomicReference<Throwable> loadFailure = new AtomicReference<>();
            CountDownLatch loadFinished = new CountDownLatch(1);
            Thread loadingThread = new Thread(() -> {
                try {
                    load.invoke(null, columns, 0, executor);
                } catch (InvocationTargetException exception) {
                    loadFailure.set(exception.getCause());
                } catch (Throwable failure) {
                    loadFailure.set(failure);
                } finally {
                    loadFinished.countDown();
                }
            }, "hardwood-rejected-load-caller");
            loadingThread.setDaemon(true);

            try {
                loadingThread.start();
                assertTrue(executor.awaitRejection(5, TimeUnit.SECONDS));
                assertEquals(1L, loadFinished.getCount(),
                        "rejection must not escape before accepted work ends");

                executor.releaseAcceptedTask();
                assertTrue(loadFinished.await(5, TimeUnit.SECONDS));
                assertSame(executor.rejection, loadFailure.get());
                assertEquals(2, executor.submissionCount());
            } finally {
                executor.releaseAcceptedTask();
                loadingThread.join(TimeUnit.SECONDS.toMillis(5));
            }
            assertFalse(loadingThread.isAlive());
        }

        FutureTask<String> stillUsable = new FutureTask<>(
                () -> "still usable");
        executor.execute(stillUsable);
        assertEquals("still usable", stillUsable.get());
    }

    @Test
    void columnAppenderTaskFailureIsPropagated() throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("failing.FailureProjection", """
                package failing;

                @io.github.jutil.columnarprojection.ProjectionSchema
                @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                public interface FailureProjection {
                    int value();
                }
                """);
        sources.put("failing.FailureProjectionStore", """
                package failing;

                public interface FailureProjectionStore extends
                        io.github.jutil.columnarprojection.ProjectionStore<
                                FailureProjection> {
                    static FailureProjectionStore create(int expectedSize) {
                        return new FailureProjection__ColumnarProjectionStore(
                                expectedSize);
                    }

                    ColumnWriter columnAppender();

                    Batch batch(int fromIndex, int toIndex);

                    interface ColumnWriter {
                        void value(int[] source, int fromIndex, int toIndex);
                    }

                    interface Batch {
                        Batch value(int[] source);
                        void append();
                    }
                }
                """);
        sources.put("failing.FailureProjection__ColumnarProjectionStore", """
                package failing;

                public final class FailureProjection__ColumnarProjectionStore
                        implements FailureProjectionStore {
                    private final ColumnWriter writer = (source, from, to) -> {
                        throw new IllegalStateException("appender task failed");
                    };

                    public FailureProjection__ColumnarProjectionStore(
                            int expectedSize) {
                    }

                    @Override
                    public ColumnWriter columnAppender() {
                        return writer;
                    }

                    @Override
                    public Batch batch(int fromIndex, int toIndex) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void add(FailureProjection value) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public int size() {
                        return 0;
                    }

                    @Override
                    public void seal() {
                    }

                    @Override
                    public io.github.jutil.columnarprojection.ProjectionCursor<
                            FailureProjection> cursor() {
                        return null;
                    }

                    @Override
                    public FailureProjection viewAt(int index) {
                        return null;
                    }
                }
                """);

        CompilerTestSupport.Compilation compilation =
                CompilerTestSupport.compile(
                        temporaryDirectory.resolve("failing-appender"),
                        sources,
                        List.of(new HardwoodProjectionProcessor()));
        assertTrue(compilation.successful(), compilation.messages());

        Path parquet = temporaryDirectory.resolve("failing-appender.parquet");
        writeInts(parquet, 101);
        try (URLClassLoader classLoader = compilation.classLoader()) {
            Class<?> loader = classLoader.loadClass(
                    "failing.FailureProjectionHardwoodLoader");
            ColumnProjection projection = (ColumnProjection) loader
                    .getMethod("projection")
                    .invoke(null);
            CountingDirectExecutor executor = new CountingDirectExecutor();
            try (ParquetFileReader reader = ParquetFileReader.open(
                        InputFile.of(parquet));
                 ColumnReaders columns = reader.buildColumnReaders(projection)
                        .batchSize(1)
                        .build()) {
                Method load = loader.getMethod(
                        "load",
                        ColumnReaders.class,
                        int.class,
                        Executor.class);

                IllegalStateException failure = assertInvocationFailure(
                        IllegalStateException.class,
                        load,
                        columns,
                        0,
                        executor);

                assertEquals("appender task failed", failure.getMessage());
                assertEquals(1, executor.submissionCount);
                assertEquals(1, executor.completionCount);
            }
        }
    }

    @Test
    void nullExecutorIsRejectedBeforeInputIsConsumed() throws Exception {
        Path parquet = temporaryDirectory.resolve("null-executor.parquet");
        writeInts(parquet, 80, 81, 82);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        ColumnProjection projection = (ColumnProjection) loader
                .getMethod("projection")
                .invoke(null);

        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(parquet));
             ColumnReaders columns = reader.buildColumnReaders(projection)
                    .batchSize(1)
                    .build()) {
            Method executorLoad = loader.getMethod(
                    "load", ColumnReaders.class, int.class, Executor.class);
            InvocationTargetException failure = assertThrows(
                    InvocationTargetException.class,
                    () -> executorLoad.invoke(null, columns, 0, null));
            assertInstanceOf(NullPointerException.class, failure.getCause());

            ProjectionStore<?> store = (ProjectionStore<?>) loader
                    .getMethod("load", ColumnReaders.class, int.class)
                    .invoke(null, columns, 0);
            assertIntValues(store, 80, 81, 82);
        }

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(parquet))) {
            Method executorLoad = loader.getMethod(
                    "load", ParquetFileReader.class, Executor.class);
            InvocationTargetException failure = assertThrows(
                    InvocationTargetException.class,
                    () -> executorLoad.invoke(null, reader, null));
            assertInstanceOf(NullPointerException.class, failure.getCause());

            ProjectionStore<?> store = (ProjectionStore<?>) loader
                    .getMethod("load", ParquetFileReader.class)
                    .invoke(null, reader);
            assertIntValues(store, 80, 81, 82);
        }
    }

    @Test
    void returnsSealedEmptyStore() throws Exception {
        Path parquet = temporaryDirectory.resolve("empty.parquet");
        writeAllMappings(parquet, 0);

        ProjectionStore<?> store = loadWithParquetReader(
                "example.AllMappingsHardwoodLoader", parquet);

        assertEquals(0, store.size());
        assertNotNull(store.cursor());
    }

    @Test
    void singleFileConvenienceLoadPreservesRowsInOrder() throws Exception {
        Path parquet = temporaryDirectory.resolve("single-file.parquet");
        writeInts(parquet, 7, 8, 9);

        ProjectionStore<?> store = loadWithParquetReader(
                "example.IntProjectionHardwoodLoader", parquet);

        assertIntValues(store, 7, 8, 9);
        assertEquals(3, capacity(store));
    }

    @Test
    void multiFileConvenienceLoadUsesCombinedCapacityAndPreservesOrder()
            throws Exception {
        Path first = temporaryDirectory.resolve("multi-first.parquet");
        Path second = temporaryDirectory.resolve("multi-second.parquet");
        writeInts(first, 10, 11, 12, 13);
        writeInts(second, 20);

        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(
                InputFile.of(first), InputFile.of(second)))) {
            assertTrue(reader.isMultiFile());
            assertEquals(2, reader.getFileCount());

            ProjectionStore<?> store = (ProjectionStore<?>) loader
                    .getMethod("load", ParquetFileReader.class)
                    .invoke(null, reader);

            assertIntValues(store, 10, 11, 12, 13, 20);
            assertEquals(5, capacity(store),
                    "exact combined sizing avoids growth from 4 to 7");
        }
    }

    @Test
    void multiFileConvenienceLoadUsesSecondFileCapacityAfterEmptyFirst()
            throws Exception {
        Path first = temporaryDirectory.resolve("multi-empty-first.parquet");
        Path second = temporaryDirectory.resolve("multi-after-empty.parquet");
        writeInts(first);
        writeInts(second, 30, 31, 32);

        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(
                InputFile.of(first), InputFile.of(second)))) {
            ProjectionStore<?> store = (ProjectionStore<?>) loader
                    .getMethod("load", ParquetFileReader.class)
                    .invoke(null, reader);

            assertIntValues(store, 30, 31, 32);
            assertEquals(3, capacity(store));
        }
    }

    @Test
    void laterMetadataFailureBecomesUncheckedIOExceptionWithOriginalCause()
            throws Exception {
        Path first = temporaryDirectory.resolve("metadata-first.parquet");
        Path second = temporaryDirectory.resolve("metadata-failing.parquet");
        writeInts(first, 40);
        writeInts(second, 50);
        IOException original = new IOException("later metadata failure");
        InputFile failing = new MetadataFailingInputFile(
                InputFile.of(second), original);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");

        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(
                InputFile.of(first), failing))) {
            InvocationTargetException invocation = assertThrows(
                    InvocationTargetException.class,
                    () -> loader.getMethod("load", ParquetFileReader.class)
                            .invoke(null, reader));

            UncheckedIOException failure = assertInstanceOf(
                    UncheckedIOException.class, invocation.getCause());
            assertSame(original, failure.getCause());
        }
    }

    @Test
    void negativeExpectedSizeDoesNotConsumeCallerReaders() throws Exception {
        Path parquet = temporaryDirectory.resolve("negative-size.parquet");
        writeAllMappings(parquet, 5);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.AllMappingsHardwoodLoader");
        Method projectionMethod = loader.getMethod("projection");
        ColumnProjection projection =
                (ColumnProjection) projectionMethod.invoke(null);

        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(parquet));
             ColumnReaders columns = reader.buildColumnReaders(projection)
                    .batchSize(2)
                    .build()) {
            Method load = loader.getMethod(
                    "load", ColumnReaders.class, int.class);
            InvocationTargetException failure = assertThrows(
                    InvocationTargetException.class,
                    () -> load.invoke(null, columns, -1));
            assertInstanceOf(IllegalArgumentException.class,
                    failure.getCause());

            ProjectionStore<?> store = (ProjectionStore<?>) load.invoke(
                    null, columns, 0);
            assertEquals(5, store.size(),
                    "the failed call must not advance the first batch");
        }
    }

    @Test
    void parquetOverloadLeavesCallerReaderOpen() throws Exception {
        Path parquet = temporaryDirectory.resolve("ownership.parquet");
        writeAllMappings(parquet, 2);
        Class<?> loader = generatedClassLoader.loadClass(
                "example.AllMappingsHardwoodLoader");
        Method load = loader.getMethod("load", ParquetFileReader.class);
        ColumnProjection projection = (ColumnProjection) loader
                .getMethod("projection")
                .invoke(null);

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(parquet))) {
            ProjectionStore<?> store = (ProjectionStore<?>) load.invoke(
                    null, reader);
            assertEquals(2, store.size());
            try (ColumnReaders secondRead = reader
                    .buildColumnReaders(projection)
                    .batchSize(1)
                    .build()) {
                assertTrue(secondRead.nextBatch(),
                        "the caller-owned Parquet reader must remain usable");
            }
        }
    }

    @Test
    void rejectsWrongPhysicalTypeBeforeReading() throws Exception {
        assertValidationFailure(
                "wrong-type.parquet",
                "message test { required int64 value; }",
                group -> group.add("value", 7L),
                "has physical type INT64; expected INT32");
    }

    @Test
    void mappingFailureLeavesFirstCallerOwnedBatchUnconsumed()
            throws Exception {
        Path parquet = temporaryDirectory.resolve("unconsumed.parquet");
        write(parquet,
                "message test { required int64 value; }",
                List.of(group -> group.add("value", 7L)));
        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        ColumnProjection projection = (ColumnProjection) loader
                .getMethod("projection")
                .invoke(null);

        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(parquet));
             ColumnReaders columns = reader.buildColumnReaders(projection)
                    .batchSize(1)
                    .build()) {
            Method load = loader.getMethod(
                    "load", ColumnReaders.class, int.class);
            InvocationTargetException failure = assertThrows(
                    InvocationTargetException.class,
                    () -> load.invoke(null, columns, 0));
            assertInstanceOf(IllegalArgumentException.class,
                    failure.getCause());
            assertTrue(columns.nextBatch(),
                    "validation must finish before the first advance");
        }
    }

    @Test
    void rejectsOptionalPrimitiveBeforeReading() throws Exception {
        assertValidationFailure(
                "optional-primitive.parquet",
                "message test { optional int32 value; }",
                group -> group.add("value", 7),
                "requires a REQUIRED Parquet column, but it is OPTIONAL");
    }

    @Test
    void rejectsNestedColumnBeforeReading() throws Exception {
        assertValidationFailure(
                "nested.parquet",
                """
                        message test {
                          required group value {
                            required int32 inner;
                          }
                        }
                        """,
                group -> group.addGroup("value").add("inner", 7),
                "nested");
    }

    @Test
    void rejectsRepeatedColumnBeforeReading() throws Exception {
        assertValidationFailure(
                "repeated.parquet",
                "message test { repeated int32 value; }",
                group -> {
                    group.add("value", 7);
                    group.add("value", 8);
                },
                "nested or repeated");
    }

    @Test
    void reportsMissingColumn() throws Exception {
        Path parquet = temporaryDirectory.resolve("missing.parquet");
        write(parquet,
                "message test { required int32 other; }",
                List.of(group -> group.add("other", 7)));

        Throwable failure = invokeParquetLoadFailure(
                "example.IntProjectionHardwoodLoader", parquet);

        assertTrue(allMessages(failure).contains("value"),
                allMessages(failure));
    }

    private ProjectionStore<?> loadAdvanced(
            String loaderName,
            Path parquet,
            int batchSize,
            int expectedSize) throws Exception {
        Class<?> loader = generatedClassLoader.loadClass(loaderName);
        ColumnProjection projection = (ColumnProjection) loader
                .getMethod("projection")
                .invoke(null);
        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(parquet));
             ColumnReaders columns = reader.buildColumnReaders(projection)
                    .batchSize(batchSize)
                    .build()) {
            return (ProjectionStore<?>) loader
                    .getMethod("load", ColumnReaders.class, int.class)
                    .invoke(null, columns, expectedSize);
        }
    }

    private ProjectionStore<?> loadAdvanced(
            String loaderName,
            Path parquet,
            int batchSize,
            int expectedSize,
            Executor executor) throws Exception {
        Class<?> loader = generatedClassLoader.loadClass(loaderName);
        ColumnProjection projection = (ColumnProjection) loader
                .getMethod("projection")
                .invoke(null);
        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(parquet));
             ColumnReaders columns = reader.buildColumnReaders(projection)
                    .batchSize(batchSize)
                    .build()) {
            return (ProjectionStore<?>) loader
                    .getMethod(
                            "load",
                            ColumnReaders.class,
                            int.class,
                            Executor.class)
                    .invoke(null, columns, expectedSize, executor);
        }
    }

    private ProjectionStore<?> loadWithParquetReader(
            String loaderName, Path parquet) throws Exception {
        Class<?> loader = generatedClassLoader.loadClass(loaderName);
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(parquet))) {
            return (ProjectionStore<?>) loader
                    .getMethod("load", ParquetFileReader.class)
                    .invoke(null, reader);
        }
    }

    private void assertValidationFailure(
            String fileName,
            String schema,
            Row row,
            String expectedMessage) throws Exception {
        Path parquet = temporaryDirectory.resolve(fileName);
        write(parquet, schema, List.of(row));

        Throwable failure = invokeParquetLoadFailure(
                "example.IntProjectionHardwoodLoader", parquet);

        assertInstanceOf(IllegalArgumentException.class, failure);
        assertTrue(allMessages(failure).contains(expectedMessage),
                allMessages(failure));
    }

    private Throwable invokeParquetLoadFailure(
            String loaderName, Path parquet) throws Exception {
        Class<?> loader = generatedClassLoader.loadClass(loaderName);
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(parquet))) {
            InvocationTargetException invocation = assertThrows(
                    InvocationTargetException.class,
                    () -> loader
                            .getMethod("load", ParquetFileReader.class)
                            .invoke(null, reader));
            return invocation.getCause();
        }
    }

    private static Object invoke(
            Class<?> projection, Object view, String method) throws Exception {
        return projection.getMethod(method).invoke(view);
    }

    private static <T extends Throwable> T assertInvocationFailure(
            Class<T> expectedType,
            Method method,
            Object... arguments) {
        InvocationTargetException invocation = assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(null, arguments));
        return assertInstanceOf(expectedType, invocation.getCause());
    }

    private void assertIntValues(ProjectionStore<?> store, int... expected)
            throws Exception {
        assertEquals(expected.length, store.size());
        Class<?> projection = generatedClassLoader.loadClass(
                "example.IntProjection");
        for (int row = 0; row < expected.length; row++) {
            assertEquals(expected[row],
                    invoke(projection, store.viewAt(row), "value"));
        }
    }

    private static String allMessages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = failure;
                current != null;
                current = current.getCause()) {
            if (messages.length() != 0) {
                messages.append(" | ");
            }
            messages.append(current.getClass().getSimpleName())
                    .append(": ")
                    .append(current.getMessage());
        }
        return messages.toString();
    }

    private static int capacity(ProjectionStore<?> store) throws Exception {
        Field capacity = store.getClass().getDeclaredField("capacity");
        capacity.setAccessible(true);
        return capacity.getInt(store);
    }

    private static final class CountingDirectExecutor implements Executor {

        private int submissionCount;
        private int completionCount;

        @Override
        public void execute(Runnable command) {
            submissionCount++;
            command.run();
            completionCount++;
        }
    }

    private static final class InterruptingFirstTaskExecutor
            implements Executor {

        private final AtomicInteger submissions = new AtomicInteger();
        private final CountDownLatch interruptSent = new CountDownLatch(1);
        private final CountDownLatch releaseFirstTask = new CountDownLatch(1);
        private volatile Thread loadingThread;

        void setLoadingThread(Thread thread) {
            loadingThread = thread;
        }

        @Override
        public void execute(Runnable command) {
            if (submissions.incrementAndGet() != 1) {
                command.run();
                return;
            }
            Thread worker = new Thread(() -> {
                loadingThread.interrupt();
                interruptSent.countDown();
                awaitUninterruptibly(releaseFirstTask);
                command.run();
            }, "hardwood-interrupting-copy-worker");
            worker.setDaemon(true);
            worker.start();
        }

        boolean awaitInterrupt(long timeout, TimeUnit unit)
                throws InterruptedException {
            return interruptSent.await(timeout, unit);
        }

        void releaseFirstTask() {
            releaseFirstTask.countDown();
        }

        int submissionCount() {
            return submissions.get();
        }
    }

    private static final class RejectAfterAcceptedTaskExecutor
            implements Executor {

        private final RejectedExecutionException rejection =
                new RejectedExecutionException("rejected copy");
        private final AtomicInteger submissions = new AtomicInteger();
        private final CountDownLatch rejectionSent = new CountDownLatch(1);
        private final CountDownLatch releaseAcceptedTask =
                new CountDownLatch(1);

        @Override
        public void execute(Runnable command) {
            int submission = submissions.incrementAndGet();
            if (submission == 1) {
                Thread worker = new Thread(() -> {
                    awaitUninterruptibly(releaseAcceptedTask);
                    command.run();
                }, "hardwood-accepted-appender-worker");
                worker.setDaemon(true);
                worker.start();
                return;
            }
            if (submission == 2) {
                rejectionSent.countDown();
                throw rejection;
            }
            command.run();
        }

        boolean awaitRejection(long timeout, TimeUnit unit)
                throws InterruptedException {
            return rejectionSent.await(timeout, unit);
        }

        void releaseAcceptedTask() {
            releaseAcceptedTask.countDown();
        }

        int submissionCount() {
            return submissions.get();
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException exception) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void writeAllMappings(Path path, int rowCount)
            throws IOException {
        List<Row> rows = new java.util.ArrayList<>();
        for (int row = 0; row < rowCount; row++) {
            int index = row;
            rows.add(group -> {
                group.add("flag", index % 2 == 0);
                group.add("count", 10 + index);
                group.add("total", 1000L + index);
                group.add("ratio", 1.5F + index);
                group.add("price", 20.25D + index);
                if (index != 1 && index != 4) {
                    group.add("text", "text-" + index);
                }
                if (index != 2) {
                    group.add("blob", Binary.fromConstantByteArray(
                            new byte[]{(byte) index, (byte) (index + 1)}));
                }
                group.add("fixedText", Binary.fromConstantByteArray(
                        ("F00" + index).getBytes(StandardCharsets.UTF_8)));
                if (index != 3) {
                    group.add("fixedBlob", Binary.fromConstantByteArray(
                            new byte[]{7, 8, (byte) index}));
                }
            });
        }
        write(path, ALL_MAPPINGS_SCHEMA, rows);
    }

    private static void writeInts(Path path, int... values)
            throws IOException {
        List<Row> rows = new java.util.ArrayList<>();
        for (int value : values) {
            rows.add(group -> group.add("value", value));
        }
        write(path, "message test { required int32 value; }", rows);
    }

    private static void write(Path path, String schemaText, List<Row> rows)
            throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        try (ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(new NioOutputFile(path))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false)
                .withPageSize(512)
                .withRowGroupSize(2048)
                .build()) {
            for (Row row : rows) {
                SimpleGroup group = new SimpleGroup(schema);
                row.populate(group);
                writer.write(group);
            }
        }
    }

    @FunctionalInterface
    private interface Row {
        void populate(Group group);
    }

    private record MetadataFailingInputFile(
            InputFile delegate,
            IOException failure) implements InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public java.nio.ByteBuffer readRange(long offset, int length)
                throws IOException {
            throw failure;
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private record NioOutputFile(Path path) implements OutputFile {

        @Override
        public PositionOutputStream create(long blockSizeHint)
                throws IOException {
            return stream(Files.newOutputStream(
                    path, StandardOpenOption.CREATE_NEW));
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint)
                throws IOException {
            return stream(Files.newOutputStream(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING));
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 64L * 1024L * 1024L;
        }

        @Override
        public String getPath() {
            return path.toString();
        }

        private static PositionOutputStream stream(OutputStream output) {
            return new DelegatingPositionOutputStream(output) {
                private long position;

                @Override
                public long getPos() {
                    return position;
                }

                @Override
                public void write(int value) throws IOException {
                    super.write(value);
                    position++;
                }

                @Override
                public void write(byte[] bytes) throws IOException {
                    super.write(bytes);
                    position += bytes.length;
                }

                @Override
                public void write(byte[] bytes, int offset, int length)
                        throws IOException {
                    super.write(bytes, offset, length);
                    position += length;
                }
            };
        }
    }
}
