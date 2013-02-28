package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.app.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.internal.api.io.Schema;
import com.google.common.reflect.TypeToken;

/**
 *
 */
public interface OutputEmitterFactory {

  /**
   * Creates a new {@link OutputEmitter} that can emit data of the given schema. The type information
   * of the {@link OutputEmitter} is passed through the {@code type} parameter.
   * @param type
   * @param flowletContext
   * @param queueProducer
   * @param queueName
   * @param schema
   * @param <T>
   * @return
   */
  <T> OutputEmitter<T> create(TypeToken<OutputEmitter<T>> type,
                              BasicFlowletContext flowletContext,
                              QueueProducer queueProducer,
                              QueueName queueName,
                              Schema schema);
}
