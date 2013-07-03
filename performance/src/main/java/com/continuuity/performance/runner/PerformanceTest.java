/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.runner;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can be used to define methods for performance testing.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface PerformanceTest {

  /**
   * Default empty exception.
   */
  static class None extends Throwable {
    private static final long serialVersionUID = 1L;
    private None() {
    }
  }

  Class<? extends Throwable> expected() default None.class;
}
