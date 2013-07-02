package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to annotate a {@link com.continuuity.api.flow.flowlet.Flowlet Flowlet}'s method to indicate that it will
 * process its input in batch.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Batch {
  /**
   * Declare the maximum number of objects that can be processed in a batch.
   */
  int value();
}
