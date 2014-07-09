/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates methods for handling {@link com.continuuity.api.procedure.Procedure} calls.
 *
 * @see com.continuuity.api.procedure.Procedure
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Handle {

  /**
   * Returns an array of Procedure method names.
   */
  String[] value();
}
