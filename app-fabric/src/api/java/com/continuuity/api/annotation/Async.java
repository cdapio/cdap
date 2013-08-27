package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark a {@link com.continuuity.api.flow.flowlet.Flowlet Flowlet} to run in asynchronous mode.
 *
 * @deprecated This is no longer supported. Using it will have no effect on the flowlet execution and it would behaves
 * the same as not having it.
 */
@Beta
@Deprecated
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Async {
}
