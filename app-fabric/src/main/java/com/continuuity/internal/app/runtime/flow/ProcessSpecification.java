package com.continuuity.internal.app.runtime.flow;

import com.continuuity.app.queue.QueueReader;
import com.google.common.base.Function;
import com.google.common.base.Objects;

import java.nio.ByteBuffer;

/**
 *
 */
final class ProcessSpecification<T> {

  private final QueueReader queueReader;
  private final ProcessMethod processMethod;
  private final Function<ByteBuffer, T> inputDatumDecoder;

  ProcessSpecification(QueueReader queueReader, Function<ByteBuffer, T> inputDatumDecoder,
                       ProcessMethod processMethod) {
    this.queueReader = queueReader;
    this.inputDatumDecoder = inputDatumDecoder;
    this.processMethod = processMethod;
  }

  public QueueReader getQueueReader() {
    return queueReader;
  }

  public ProcessMethod getProcessMethod() {
    return processMethod;
  }

  public Function<ByteBuffer, T> getInputDecoder() {
    return inputDatumDecoder;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("queue", queueReader)
      .add("method", processMethod)
      .add("inputDatumDecoder", inputDatumDecoder)
      .toString();
  }
}
