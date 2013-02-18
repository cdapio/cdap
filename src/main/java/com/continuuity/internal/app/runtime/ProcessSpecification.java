package com.continuuity.internal.app.runtime;

import com.continuuity.app.queue.QueueReader;
import com.google.common.base.Objects;

/**
 *
 */
final class ProcessSpecification {

  private final QueueReader queueReader;
  private final ProcessMethod processMethod;

  ProcessSpecification(QueueReader queueReader, ProcessMethod processMethod) {
    this.queueReader = queueReader;
    this.processMethod = processMethod;
  }

  public QueueReader getQueueReader() {
    return queueReader;
  }

  public ProcessMethod getProcessMethod() {
    return processMethod;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("queue", queueReader)
      .add("method", processMethod).toString();
  }
}
