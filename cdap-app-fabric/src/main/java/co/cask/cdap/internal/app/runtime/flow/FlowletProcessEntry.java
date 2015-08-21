/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.internal.app.runtime.flow;

import com.google.common.math.LongMath;
import com.google.common.primitives.Longs;

import java.util.concurrent.TimeUnit;

/**
 * Data structure for tracking retry and backing off calls to process method in flowlet.
 */
final class FlowletProcessEntry<T> implements Comparable<FlowletProcessEntry> {

  // Minimum back-off time in nanoseconds, 1ms.
  private static final long BACKOFF_MIN = TimeUnit.MILLISECONDS.toNanos(1);

  // Maximum back-off time in nanoseconds when increasing exponentially, 100ms.
  private static final long BACKOFF_MAX = TimeUnit.MILLISECONDS.toNanos(100);

  // Start time for switching from constant to exponentially increasing back-off time, 20ms.
  private static final long BACKOFF_EXP_START = TimeUnit.MILLISECONDS.toNanos(20);

  // Incrementing back-off time by this until reaching exponential increase range.
  private static final long BACKOFF_CONSTANT_INCREMENT = TimeUnit.MILLISECONDS.toNanos(1);

  // Doubling back-off time during exponential increase, up to maximum back-off time.
  private static final int BACKOFF_EXP = 2;

  private final ProcessSpecification<T> processSpec;
  private final ProcessSpecification<T> retrySpec;
  private final boolean isTick;

  /**
   * {@code System.nanoTime} when the next deque should happen.
   */
  private long nextDeque;
  private long currentBackOff = BACKOFF_MIN;

  static <T> FlowletProcessEntry<T> create(ProcessSpecification<T> processSpec) {
    long nextDeque;
    try {
      nextDeque = LongMath.checkedAdd(System.nanoTime(), processSpec.getInitialCallDelay());
    } catch (ArithmeticException e) {
      // in case System.nanoTime + initialCallDelay > MAX_VALUE, we simply set nextDeque to MAX_VALUE
      // so that the flowlet never performs a deque instead of dequing immediately.
      nextDeque = Long.MAX_VALUE;
    }
    return new FlowletProcessEntry<>(processSpec, null, nextDeque);
  }

  static <T> FlowletProcessEntry<T> create(ProcessSpecification<T> processSpec, ProcessSpecification<T> retrySpec) {
    return new FlowletProcessEntry<>(processSpec, retrySpec, 0);
  }

  private FlowletProcessEntry(ProcessSpecification<T> processSpec, ProcessSpecification<T> retrySpec, long nextDeque) {
    this.processSpec = processSpec;
    this.retrySpec = retrySpec;
    this.nextDeque = nextDeque;
    this.isTick = processSpec.isTick();
  }

  long getNextDeque() {
    return nextDeque;
  }

  public boolean isRetry() {
    return retrySpec != null;
  }

  public void await() throws InterruptedException {
    long waitTime = nextDeque - System.nanoTime();
    if (waitTime > 0) {
      TimeUnit.NANOSECONDS.sleep(waitTime);
    }
  }

  public boolean shouldProcess() {
    return nextDeque - System.nanoTime() <= 0;
  }

  @Override
  public int compareTo(FlowletProcessEntry o) {
    return Longs.compare(nextDeque, o.nextDeque);
  }

  public void resetBackOff() {
    nextDeque = System.nanoTime() + processSpec.getCallDelay();
    currentBackOff = BACKOFF_MIN;
  }

  public void backOff() {
    nextDeque = System.nanoTime() + currentBackOff;
    if (currentBackOff < BACKOFF_EXP_START) {
      currentBackOff += BACKOFF_CONSTANT_INCREMENT;
    } else {
      currentBackOff = Math.min(currentBackOff * BACKOFF_EXP, BACKOFF_MAX);
    }
  }

  public ProcessSpecification<T> getProcessSpec() {
    return retrySpec == null ? processSpec : retrySpec;
  }

  public FlowletProcessEntry<T> resetRetry() {
    return retrySpec == null ? this : new FlowletProcessEntry<>(processSpec, null, processSpec.getCallDelay());
  }

  public boolean isTick() {
    return isTick;
  }
}
