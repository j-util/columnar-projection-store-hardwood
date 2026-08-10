package io.github.jutil.columnarprojection.hardwood.processor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    }

    @Test
    void multiFileConvenienceLoadGrowsPastFirstFileHintAndPreservesOrder()
            throws Exception {
        Path first = temporaryDirectory.resolve("multi-first.parquet");
        Path second = temporaryDirectory.resolve("multi-second.parquet");
        writeInts(first, 10);
        writeInts(second, 20, 21, 22);

        Class<?> loader = generatedClassLoader.loadClass(
                "example.IntProjectionHardwoodLoader");
        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(
                InputFile.of(first), InputFile.of(second)))) {
            assertTrue(reader.isMultiFile());
            assertEquals(1, reader.getFileMetaData().numRows(),
                    "Hardwood exposes the first file's row count");

            ProjectionStore<?> store = (ProjectionStore<?>) loader
                    .getMethod("load", ParquetFileReader.class)
                    .invoke(null, reader);

            assertIntValues(store, 10, 20, 21, 22);
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
