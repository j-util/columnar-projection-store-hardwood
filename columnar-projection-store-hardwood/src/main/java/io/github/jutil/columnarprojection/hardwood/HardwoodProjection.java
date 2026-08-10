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
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
@Documented
public @interface HardwoodProjection {
}
