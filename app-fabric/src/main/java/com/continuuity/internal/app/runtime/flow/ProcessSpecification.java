package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.annotation.Tick;
import com.continuuity.app.queue.QueueReader;
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
   * Returns the delay in nanoseconds. Should only applicable to {@link com.continuuity.api.annotation.Tick} method.
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
