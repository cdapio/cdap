package com.continuuity.internal.app.runtime;

import com.continuuity.api.io.Schema;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public final class ReflectionOutputEmitter implements ManagedOutputEmitter<Object> {

  private final QueueProducer queueProducer;
  private final URI queueName;
  private final byte[] schemaHash;
  private final ReflectionDatumWriter writer;
  private final Queue<EmittedDatum> dataQueue;
  private final AtomicReference<Queue<EmittedDatum>> dataQueueRef;

  public ReflectionOutputEmitter(QueueProducer queueProducer, URI queueName, Schema schema) {
    this.queueProducer = queueProducer;
    this.queueName = queueName;
    this.schemaHash = schema.getSchemaHash().toByteArray();
    this.writer = new ReflectionDatumWriter(schema);
    this.dataQueue = new ConcurrentLinkedQueue<EmittedDatum>();
    this.dataQueueRef = new AtomicReference<Queue<EmittedDatum>>(dataQueue);
  }

  @Override
  public void emit(Object data) {
    emit(data, ImmutableMap.<String, Object>of());
  }

  @Override
  public void emit(Object data, Map<String, Object> partitions) {
    Queue<EmittedDatum> queue = dataQueueRef.get();
    Preconditions.checkState(queue != null, "OutputEmitter is captured. No output is allowed until reset.");

    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      output.write(schemaHash);
      writer.write(data, new BinaryEncoder(output));
      queue.add(new EmittedDatum(queueProducer, queueName, output.toByteArray(), partitions));
    } catch(IOException e) {
      // This should never happens.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<EmittedDatum> capture() {
    return ImmutableList.copyOf(dataQueueRef.getAndSet(null));
  }

  @Override
  public void reset() {
    dataQueue.clear();
    dataQueueRef.set(dataQueue);
  }
}
