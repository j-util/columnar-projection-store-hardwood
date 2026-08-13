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
                "does not expose the required static create(int expectedSize, "
                        + "Executor executor) factory"),
                compilation.messages());
        assertTrue(compilation.messages().contains(
                "columnar-projection-store-processor 1.3.0 or newer"),
                compilation.messages());
        assertFalse(compilation.messages().contains(
                "did not generate the store contract"),
                compilation.messages());
    }
}
