/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to tag a {@link com.continuuity.api.flow.flowlet.Flowlet} process method.
 *
 * @see com.continuuity.api.flow.flowlet.Flowlet
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ProcessInput {

  /**
   * Optionally tag the name of inputs to the process method.
   *
   * @return Array of input names.
   */
  String[] value() default {};

  /**
   * Optionally declares the name of the partition key for data partitioning to the process methods
   * across multiple instances of {@link com.continuuity.api.flow.flowlet.Flowlet}.
   *
   * @return Name of the partition key.
   */
  @Deprecated
  String partition() default "";
}
