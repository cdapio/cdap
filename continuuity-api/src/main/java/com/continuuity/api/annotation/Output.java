/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare the name for a {@link com.continuuity.api.flow.flowlet.OutputEmitter}.
 *
 * @see com.continuuity.api.flow.flowlet.Flowlet Flowlet
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Output {

  /**
   * Returns the name of the output.
   */
  String value();
}
