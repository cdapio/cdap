/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.app.queue.QueueReader;
import com.google.common.base.Objects;

import java.util.concurrent.TimeUnit;

/**
 *
 */
final class ProcessSpecification<T> {

  private final QueueReader<T> queueReader;
  private final ProcessMethod<T> processMethod;
  private final Tick tickAnnotation;
  private final boolean isTick;

  ProcessSpecification(QueueReader<T> queueReader, ProcessMethod<T> processMethod, Tick tickAnnotation) {
    this.queueReader = queueReader;
    this.processMethod = processMethod;
    this.tickAnnotation = tickAnnotation;
    this.isTick = tickAnnotation != null;
  }

  QueueReader<T> getQueueReader() {
    return queueReader;
  }

  ProcessMethod<T> getProcessMethod() {
    return processMethod;
  }

  long getInitialCallDelay() {
    return isTick ? convertToNano(tickAnnotation.initialDelay(), tickAnnotation.unit()) : 0L;
  }

  /**
   * Returns the delay in nanoseconds. Should only applicable to {@link co.cask.cdap.api.annotation.Tick} method.
   * @return delay time in nanoseconds.
   */
  long getCallDelay() {
    return isTick ? convertToNano(tickAnnotation.delay(), tickAnnotation.unit()) : 0L;
  }

  boolean isTick() {
    return isTick;
  }

  private long convertToNano(long time, TimeUnit unit) {
    return TimeUnit.NANOSECONDS.convert(time, unit);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("queue", queueReader)
      .add("method", processMethod)
      .toString();
  }
}
