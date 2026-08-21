package io.github.jutil.columnarprojection.hardwood.processor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HardwoodProcessorVersionCompatibilityTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void reportsOutdatedGeneratedStoreContractActionably() throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("example.PriceProjection", """
                package example;

                @io.github.jutil.columnarprojection.ProjectionSchema
                @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                public interface PriceProjection {
                    int value();
                }
                """);
        sources.put("example.PriceProjectionBatch", """
                package example;

                public interface PriceProjectionBatch {
                    PriceProjectionBatch value(int[] values);
                }
                """);
        sources.put("example.PriceProjectionStore", """
                package example;

                public interface PriceProjectionStore extends
                        io.github.jutil.columnarprojection.ProjectionStore<
                                PriceProjection> {
                    PriceProjectionBatch batch(int fromIndex, int toIndex);
                }
                """);
        sources.put(
                "example.PriceProjection__ColumnarProjectionStore",
                """
                package example;

                public abstract class
                        PriceProjection__ColumnarProjectionStore
                        implements PriceProjectionStore {
                    public PriceProjection__ColumnarProjectionStore(
                            int expectedSize) {
                    }
                }
                """);

        CompilerTestSupport.Compilation compilation =
                CompilerTestSupport.compile(
                        temporaryDirectory.resolve("outdated-core"),
                        sources,
                        List.of(new HardwoodProjectionProcessor()));

        assertFalse(compilation.successful(), compilation.messages());
        assertTrue(compilation.messages().contains(
                "lacks the required Columnar Projection Store 1.3 "
                        + "column-appender API"),
                compilation.messages());
        assertTrue(compilation.messages().contains(
                "public static create(int expectedSize)"),
                compilation.messages());
        assertTrue(compilation.messages().contains(
                "columnAppender() returning the generated appender contract"),
                compilation.messages());
        assertFalse(compilation.messages().contains(
                "did not generate the store contract"),
                compilation.messages());
    }

    @Test
    void reportsMissingRangedColumnAppenderMethodPrecisely() throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("example.PriceProjection", """
                package example;

                @io.github.jutil.columnarprojection.ProjectionSchema
                @io.github.jutil.columnarprojection.hardwood.HardwoodProjection
                public interface PriceProjection {
                    int value();
                }
                """);
        sources.put("example.PriceProjectionBatch", """
                package example;

                public interface PriceProjectionBatch {
                    PriceProjectionBatch value(int[] values);
                }
                """);
        sources.put("example.PriceProjectionAppender", """
                package example;

                public interface PriceProjectionAppender {
                    void value(int[] values);
                }
                """);
        sources.put("example.PriceProjectionStore", """
                package example;

                public interface PriceProjectionStore extends
                        io.github.jutil.columnarprojection.ProjectionStore<
                                PriceProjection> {
                    static PriceProjectionStore create(int expectedSize) {
                        return null;
                    }

                    PriceProjectionAppender columnAppender();

                    PriceProjectionBatch batch(int fromIndex, int toIndex);
                }
                """);
        sources.put(
                "example.PriceProjection__ColumnarProjectionStore",
                """
                package example;

                public abstract class
                        PriceProjection__ColumnarProjectionStore
                        implements PriceProjectionStore {
                    public PriceProjection__ColumnarProjectionStore(
                            int expectedSize) {
                    }
                }
                """);

        CompilerTestSupport.Compilation compilation =
                CompilerTestSupport.compile(
                        temporaryDirectory.resolve("missing-ranged-appender"),
                        sources,
                        List.of(new HardwoodProjectionProcessor()));

        assertFalse(compilation.successful(), compilation.messages());
        assertTrue(compilation.messages().contains(
                "ranged appender method value(int[], int, int)"),
                compilation.messages());
        assertFalse(compilation.messages().contains(
                "create(int expectedSize, Executor executor)"),
                compilation.messages());
    }
}
