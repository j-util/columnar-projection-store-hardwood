package io.github.jutil.columnarprojection.hardwood;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Requests a schema-specific loader that transfers Hardwood column batches
 * directly into a generated Columnar Projection Store.
 *
 * <p>This annotation is valid only on a non-private interface that is also
 * annotated with
 * {@link io.github.jutil.columnarprojection.ProjectionSchema}. The generated
 * loader uses each effective projection accessor name as its Hardwood column
 * name. The annotation processor reports unsupported accessor types and
 * generated-name collisions at compilation time.
 *
 * <p>The generated loader provides both sequential overloads and overloads
 * that borrow a caller-owned {@link java.util.concurrent.Executor} to copy a
 * batch's independent destination columns concurrently. Hardwood cursor
 * advancement and typed getters remain on the calling thread, and the loader
 * never shuts down the borrowed executor. Convenience overloads can set the
 * maximum records per Hardwood column batch; overloads without that argument
 * retain Hardwood's automatic batch sizing.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
@Documented
public @interface HardwoodProjection {
}
