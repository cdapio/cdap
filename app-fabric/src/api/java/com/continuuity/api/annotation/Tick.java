/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * A flowlet’s method can be annotated with @Tick. Instead of processing data objects from a flowlet input, this
 * method is invoked periodically, without arguments. This can be used, for example, to generate data, or to pull
 * data from an external data source periodically on a fixed cadence.
 *
 *  <pre>
 *    <code>
 *      public class RandomSource extends AbstractFlowlet {
 *        private OutputEmitter{@literal <}Integer> randomOutput;
 *
 *        private final Random random = new Random();
 *
 *        {@literal @}Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
 *        public void generate() throws InterruptedException {
 *          randomOutput.emit(random.nextInt(10000));
 *        }
 *      }
 *    </code>
 *  </pre>
 *
 * @see com.continuuity.api.flow.flowlet.Flowlet
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Tick {

  /**
   * Initial delay before calling the tick method for the first time. Default is {@code 0}.
   * @return Time for the initial delay.
   */
  long initialDelay() default 0L;

  /**
   * The time to delay between termination of one tick call and the next one.
   * @return Time to delay between calls.
   */
  long delay();

  /**
   * The time unit for both {@link #initialDelay()} and {@link #delay()}. Default is {@link TimeUnit#SECONDS}.
   * @return The time unit.
   */
  TimeUnit unit() default TimeUnit.SECONDS;

  /**
   * Optionally specifies the maximum number of retries of failure inputs before giving up on it.
   * Defaults to 0 (no retry).
   */
  int maxRetries() default 0;
}
