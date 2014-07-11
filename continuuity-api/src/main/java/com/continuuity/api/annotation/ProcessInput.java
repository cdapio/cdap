/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation tag required for {@link com.continuuity.api.flow.flowlet.Flowlet} process methods.
 * 
 * <p>
 * Example:
 * </p>
 *
 * <pre>
 * <code>
 * OutputEmitter{@literal <}Long> output;
 *
 * {@literal @}ProcessInput
 * public void round(Double number) {
 *   output.emit(Math.round(number));
 * }
 * </code>
 * </pre>
 *
 * <p>
 * See the <i><a href="http://continuuity.com/docs/reactor/current/en/">Continuuity Reactor Developer Guides</a></i>
 * and the Reactor example applications for more information.
 * </p>
 *
 * @see com.continuuity.api.flow.flowlet.Flowlet
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ProcessInput {

  static final int DEFAULT_MAX_RETRIES = 20;

  /**
   * Optionally tags the name of inputs to the process method.
   */
  String[] value() default {};

  /**
   * Optionally specifies the maximum number of retries of failure inputs before giving up.
   * Defaults to {@link #DEFAULT_MAX_RETRIES}.
   */
  int maxRetries() default DEFAULT_MAX_RETRIES;
}
