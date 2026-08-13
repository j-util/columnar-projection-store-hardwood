package io.github.jutil.columnarprojection.hardwood.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jutil.columnarprojection.processor.ProjectionSchemaProcessor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.Processor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HardwoodProjectionProcessorTest {

    private static final String SIMPLE_SCHEMA = """
            package example;

            @io.github.jutil.columnarprojection.ProjectionSchema
            @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
            public interface PriceProjection {
                long id();
                double price();
                String symbol();
            }
            """;

    @TempDir
    Path temporaryDirectory;

    @Test
    void cleanConsumerCompilationGeneratesDirectRangedLoader() throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("example.PriceProjection", SIMPLE_SCHEMA);
        sources.put("example.Consumer", """
                package example;

                final class Consumer {
                    static PriceProjectionStore load(
                            dev.hardwood.reader.ParquetFileReader reader) {
                        return PriceProjectionHardwoodLoader.load(reader);
                    }

                    static PriceProjectionStore load(
                            dev.hardwood.reader.ParquetFileReader reader,
                            int batchSize) {
                        return PriceProjectionHardwoodLoader.load(
                                reader, batchSize);
                    }

                    static PriceProjectionStore load(
                            dev.hardwood.reader.ParquetFileReader reader,
                            java.util.concurrent.Executor executor) {
                        return PriceProjectionHardwoodLoader.load(
                                reader, executor);
                    }

                    static PriceProjectionStore load(
                            dev.hardwood.reader.ParquetFileReader reader,
                            int batchSize,
                            java.util.concurrent.Executor executor) {
                        return PriceProjectionHardwoodLoader.load(
                                reader, batchSize, executor);
                    }

                    static PriceProjectionStore load(
                            dev.hardwood.reader.ColumnReaders readers,
                            int expectedSize,
                            java.util.concurrent.Executor executor) {
                        return PriceProjectionHardwoodLoader.load(
                                readers, expectedSize, executor);
                    }
                }
                """);

        CompilerTestSupport.Compilation compilation = compile(
                temporaryDirectory.resolve("consumer"),
                sources,
                false);

        assertTrue(compilation.successful(), compilation.messages());
        String generated = compilation.generatedSource(
                "example.PriceProjectionHardwoodLoader");
        assertTrue(generated.contains(
                "PriceProjectionStore load("), generated);
        assertTrue(generated.contains(
                "new example.PriceProjection__ColumnarProjectionStore("),
                generated);
        assertTrue(generated.contains(
                "store.batch(0, recordCount)"), generated);
        assertFalse(generated.contains("store.batch()"), generated);
        assertFalse(generated.contains(".add("), generated);
        assertFalse(generated.contains("java.lang.reflect"), generated);
        assertFalse(generated.contains("Class.forName"), generated);
        assertTrue(generated.contains(
                "int fileCount = reader.getFileCount();"), generated);
        assertTrue(generated.contains(
                "for (int fileIndex = 0; fileIndex < fileCount; fileIndex++)"),
                generated);
        assertTrue(generated.contains(
                "reader.getFileMetaData(fileIndex).numRows()"), generated);
        assertTrue(generated.contains("java.lang.Math.addExact("), generated);
        assertTrue(generated.contains("java.lang.Math.toIntExact("), generated);
        assertFalse(generated.contains("reader.getFileMetaData()"), generated);
        assertTrue(generated.contains(
                "catch (java.io.IOException exception)"), generated);
        assertTrue(generated.contains(
                "throw new java.io.UncheckedIOException(exception);"),
                generated);
        assertTrue(generated.contains(
                "load(dev.hardwood.reader.ColumnReaders readers, "
                        + "int expectedSize)"),
                generated);
        assertTrue(generated.contains(
                "new example.PriceProjection__ColumnarProjectionStore("
                        + "expectedSize)"),
                generated);
        assertTrue(generated.contains(
                "example.PriceProjectionStore.create(expectedSize, executor)"),
                generated);
        assertTrue(generated.contains(
                "load(dev.hardwood.reader.ParquetFileReader reader, "
                        + "int batchSize)"),
                generated);
        assertTrue(generated.contains(
                "reader.buildColumnReaders(projection())"
                        + ".batchSize(batchSize).build()"),
                generated);
        assertTrue(generated.contains(
                "reader.buildColumnReaders(projection()).build()"),
                "default overloads must retain Hardwood's automatic sizing");
        assertFalse(generated.contains(".batchSize(0)"), generated);
        assertTrue(generated.contains(
                "@param batchSize maximum records per Hardwood column batch; "
                        + "must be greater than zero"),
                generated);
        assertTrue(generated.contains(
                "validated before file footers are read or projected "
                        + "{@code ColumnReaders} are constructed or input is "
                        + "advanced"),
                generated);
        assertTrue(generated.contains(
                "load(dev.hardwood.reader.ParquetFileReader reader, "
                        + "java.util.concurrent.Executor executor)"),
                generated);
        assertTrue(generated.contains(
                "load(dev.hardwood.reader.ParquetFileReader reader, "
                        + "int batchSize, "
                        + "java.util.concurrent.Executor executor)"),
                generated);
        assertTrue(generated.contains(
                "load(dev.hardwood.reader.ColumnReaders readers, "
                        + "int expectedSize, "
                        + "java.util.concurrent.Executor executor)"),
                generated);
        assertFalse(generated.contains("java.util.concurrent.Executors"),
                generated);
        assertFalse(generated.contains(".shutdown("), generated);
        assertFalse(generated.contains(".shutdownNow("), generated);
        assertTrue(generated.contains("batch.id(column0.getLongs())")
                || generated.contains("batch.id(column1.getLongs())")
                || generated.contains("batch.id(column2.getLongs())"),
                generated);

        try (URLClassLoader classLoader = compilation.classLoader()) {
            Class<?> loader = classLoader.loadClass(
                    "example.PriceProjectionHardwoodLoader");
            Class<?> store = classLoader.loadClass("example.PriceProjectionStore");
            Method projection = loader.getMethod("projection");
            Method readerLoad = loader.getMethod(
                    "load", dev.hardwood.reader.ParquetFileReader.class);
            Method batchSizeReaderLoad = loader.getMethod(
                    "load",
                    dev.hardwood.reader.ParquetFileReader.class,
                    int.class);
            Method advancedLoad = loader.getMethod(
                    "load", dev.hardwood.reader.ColumnReaders.class, int.class);
            Method executorReaderLoad = loader.getMethod(
                    "load",
                    dev.hardwood.reader.ParquetFileReader.class,
                    java.util.concurrent.Executor.class);
            Method batchSizeExecutorReaderLoad = loader.getMethod(
                    "load",
                    dev.hardwood.reader.ParquetFileReader.class,
                    int.class,
                    java.util.concurrent.Executor.class);
            Method executorAdvancedLoad = loader.getMethod(
                    "load",
                    dev.hardwood.reader.ColumnReaders.class,
                    int.class,
                    java.util.concurrent.Executor.class);

            assertEquals(dev.hardwood.schema.ColumnProjection.class,
                    projection.getReturnType());
            assertEquals(store, readerLoad.getReturnType());
            assertEquals(store, batchSizeReaderLoad.getReturnType());
            assertEquals(store, advancedLoad.getReturnType());
            assertEquals(store, executorReaderLoad.getReturnType());
            assertEquals(store, batchSizeExecutorReaderLoad.getReturnType());
            assertEquals(store, executorAdvancedLoad.getReturnType());
            assertEquals(0, readerLoad.getExceptionTypes().length,
                    "load(ParquetFileReader) must not declare an exception");
            assertEquals(0, batchSizeReaderLoad.getExceptionTypes().length);
            assertEquals(0, advancedLoad.getExceptionTypes().length);
            assertEquals(0, executorReaderLoad.getExceptionTypes().length);
            assertEquals(0,
                    batchSizeExecutorReaderLoad.getExceptionTypes().length);
            assertEquals(0, executorAdvancedLoad.getExceptionTypes().length);

            Set<String> publicSignatures = Arrays.stream(
                            loader.getDeclaredMethods())
                    .filter(method -> Modifier.isPublic(method.getModifiers()))
                    .map(HardwoodProjectionProcessorTest::signature)
                    .collect(Collectors.toSet());
            assertEquals(Set.of(
                    "projection()",
                    "load(dev.hardwood.reader.ParquetFileReader)",
                    "load(dev.hardwood.reader.ParquetFileReader,int)",
                    "load(dev.hardwood.reader.ParquetFileReader,"
                            + "java.util.concurrent.Executor)",
                    "load(dev.hardwood.reader.ParquetFileReader,int,"
                            + "java.util.concurrent.Executor)",
                    "load(dev.hardwood.reader.ColumnReaders,int)",
                    "load(dev.hardwood.reader.ColumnReaders,int,"
                            + "java.util.concurrent.Executor)"),
                    publicSignatures);
        }
    }

    @Test
    void processorOrderDoesNotAffectGeneration() throws Exception {
        CompilerTestSupport.Compilation columnarFirst = compile(
                temporaryDirectory.resolve("columnar-first"),
                Map.of("example.PriceProjection", SIMPLE_SCHEMA),
                false);
        CompilerTestSupport.Compilation hardwoodFirst = compile(
                temporaryDirectory.resolve("hardwood-first"),
                Map.of("example.PriceProjection", SIMPLE_SCHEMA),
                true);

        assertTrue(columnarFirst.successful(), columnarFirst.messages());
        assertTrue(hardwoodFirst.successful(), hardwoodFirst.messages());
        assertTrue(columnarFirst.generatedSource(
                "example.PriceProjectionHardwoodLoader").equals(
                hardwoodFirst.generatedSource(
                        "example.PriceProjectionHardwoodLoader")));
    }

    @Test
    void discoversCollisionSafeStoreContract() throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("example.PriceStore.Marker", """
                package example.PriceStore;
                @java.lang.annotation.Target(
                        java.lang.annotation.ElementType.TYPE)
                public @interface Marker {
                }
                """);
        sources.put("example.Price", """
                package example;

                @example.PriceStore.Marker
                @io.github.jutil.columnarprojection.ProjectionSchema
                @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                public interface Price {
                    int value();
                }
                """);

        CompilerTestSupport.Compilation compilation = compile(
                temporaryDirectory.resolve("collision-safe-store"),
                sources,
                true);

        assertTrue(compilation.successful(), compilation.messages());
        String generated = compilation.generatedSource(
                "example.PriceHardwoodLoader");
        assertTrue(generated.contains("example.PriceStore_ load("), generated);
    }

    @Test
    void resolvesInheritedCovariantAccessorsAndMemberName() throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("example.Base", """
                package example;
                interface Base<T> {
                    T value();
                }
                """);
        sources.put("example.Named", """
                package example;
                interface Named extends Base<CharSequence> {
                    @Override
                    String value();
                }
                """);
        sources.put("example.Outer", """
                package example;

                public final class Outer {
                    private Outer() {
                    }

                    @io.github.jutil.columnarprojection.ProjectionSchema
                    @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                    public interface PriceProjection extends Named {
                        long amount();
                    }
                }
                """);

        CompilerTestSupport.Compilation compilation = compile(
                temporaryDirectory.resolve("member"), sources, false);

        assertTrue(compilation.successful(), compilation.messages());
        String generated = compilation.generatedSource(
                "example.Outer$PriceProjectionHardwoodLoader");
        assertTrue(generated.contains("columns("), generated);
        assertTrue(generated.contains("\"amount\""), generated);
        assertTrue(generated.contains("\"value\""), generated);
        assertTrue(generated.contains(".getStrings()"), generated);
    }

    @Test
    void rejectsUnsupportedJavaType() throws Exception {
        String source = """
                package example;

                @io.github.jutil.columnarprojection.ProjectionSchema
                @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                public interface UnsupportedProjection {
                    Integer value();
                }
                """;
        CompilerTestSupport.Compilation compilation = compile(
                temporaryDirectory.resolve("unsupported"),
                Map.of("example.UnsupportedProjection", source),
                false);

        assertFalse(compilation.successful(), compilation.messages());
        assertTrue(compilation.messages().contains(
                "unsupported Java type java.lang.Integer"),
                compilation.messages());
        assertTrue(compilation.messages().contains(
                "boolean, int, long, float, double, String, and byte[]"),
                compilation.messages());
    }

    @Test
    void reportsMissingColumnarProcessorActionably() throws Exception {
        CompilerTestSupport.Compilation compilation =
                CompilerTestSupport.compile(
                        temporaryDirectory.resolve("missing-processor"),
                        Map.of("example.PriceProjection", SIMPLE_SCHEMA),
                        List.of(new HardwoodProjectionProcessor()));

        assertFalse(compilation.successful(), compilation.messages());
        assertTrue(compilation.messages().contains(
                "columnar-projection-store-processor:1.3.0"),
                compilation.messages());
        assertTrue(compilation.messages().contains(
                "annotation processor path"), compilation.messages());
    }

    @Test
    void preservesColumnarSchemaErrorsWithoutFalseMissingProcessorDiagnostic()
            throws Exception {
        String source = """
                package example;

                public final class Outer {
                    @io.github.jutil.columnarprojection.ProjectionSchema
                    @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                    private interface InvalidProjection<T> {
                        T value();
                    }
                }
                """;
        CompilerTestSupport.Compilation compilation = compile(
                temporaryDirectory.resolve("invalid-schema"),
                Map.of("example.Outer", source),
                false);

        assertFalse(compilation.successful(), compilation.messages());
        assertTrue(compilation.messages().contains(
                "Projection schema interfaces must not be generic"),
                compilation.messages());
        assertTrue(compilation.messages().contains(
                "Projection schemas and their enclosing types must not be "
                        + "private"),
                compilation.messages());
        assertFalse(compilation.messages().contains(
                "Columnar Projection Store's annotation processor did not "
                        + "generate the store contract"),
                compilation.messages());
    }

    @Test
    void reportsGeneratedLoaderNameCollision() throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("example.PriceProjection", SIMPLE_SCHEMA);
        sources.put("example.PriceProjectionHardwoodLoader", """
                package example;
                public final class PriceProjectionHardwoodLoader {
                    private PriceProjectionHardwoodLoader() {
                    }
                }
                """);

        CompilerTestSupport.Compilation compilation = compile(
                temporaryDirectory.resolve("loader-collision"),
                sources,
                false);

        assertFalse(compilation.successful(), compilation.messages());
        assertTrue(compilation.messages().contains(
                "Generated Hardwood loader name collision"),
                compilation.messages());
        assertTrue(compilation.messages().contains(
                "example.PriceProjectionHardwoodLoader"),
                compilation.messages());
    }

    @Test
    void requiresProjectionSchemaOnSameInterface() throws Exception {
        String source = """
                package example;
                @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                public interface NotAProjection {
                    int value();
                }
                """;
        CompilerTestSupport.Compilation compilation =
                CompilerTestSupport.compile(
                        temporaryDirectory.resolve("missing-annotation"),
                        Map.of("example.NotAProjection", source),
                        List.of(new HardwoodProjectionProcessor()));

        assertFalse(compilation.successful(), compilation.messages());
        assertTrue(compilation.messages().contains(
                "requires the same interface to be annotated with "
                        + "@ProjectionSchema"),
                compilation.messages());
    }

    private CompilerTestSupport.Compilation compile(
            Path root,
            Map<String, String> sources,
            boolean hardwoodFirst) throws Exception {
        Processor columnar = new ProjectionSchemaProcessor();
        Processor hardwood = new HardwoodProjectionProcessor();
        List<Processor> processors = hardwoodFirst
                ? List.of(hardwood, columnar)
                : List.of(columnar, hardwood);
        return CompilerTestSupport.compile(root, sources, processors);
    }

    private static String signature(Method method) {
        return method.getName() + Arrays.stream(method.getParameterTypes())
                .map(Class::getName)
                .collect(Collectors.joining(",", "(", ")"));
    }
}
