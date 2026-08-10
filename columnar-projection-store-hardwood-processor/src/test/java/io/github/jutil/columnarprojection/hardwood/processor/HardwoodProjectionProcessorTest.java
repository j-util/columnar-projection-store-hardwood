package io.github.jutil.columnarprojection.hardwood.processor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jutil.columnarprojection.processor.ProjectionSchemaProcessor;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        assertTrue(generated.contains("batch.id(column0.getLongs())")
                || generated.contains("batch.id(column1.getLongs())")
                || generated.contains("batch.id(column2.getLongs())"),
                generated);
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
                "columnar-projection-store-processor:1.2.0"),
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
}
