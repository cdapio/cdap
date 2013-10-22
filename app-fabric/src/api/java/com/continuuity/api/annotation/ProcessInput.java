/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Required annotation tag for {@link com.continuuity.api.flow.flowlet.Flowlet} process methods.
 * 
 *  <pre>
 *    <code>
 *      OutputEmitter{@literal <}Long> output;
 *
 *      {@literal @}ProcessInput
 *      public void round(Double number) {
 *        output.emit(Math.round(number));
 *      }
 *    </code>
 *  </pre>
 *
 * See the <i>Continuuity Reactor Developer Guide</i> and the Reactor example applications.
 *
 * @see com.continuuity.api.flow.flowlet.Flowlet
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ProcessInput {

  static final int DEFAULT_MAX_RETRIES = 20;

  /**
   * Optionally tag the name of inputs to the process method.
   */
  String[] value() default {};

  /**
   * Optionally specifies the maximum number of retries of failure inputs before giving up on it.
   * Defaults to {@link #DEFAULT_MAX_RETRIES}.
   */
  int maxRetries() default DEFAULT_MAX_RETRIES;
}
