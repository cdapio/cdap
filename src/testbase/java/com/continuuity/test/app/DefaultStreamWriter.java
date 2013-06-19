package com.continuuity.test.app;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.queue.QueueName;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public final class DefaultStreamWriter implements StreamWriter {

  private final OperationExecutor opex;
  private final OperationContext opCtx;
  private final QueueProducer queueProducer;
  private final QueueName queueName;
  private final StreamEventCodec codec;

  @Inject
  public DefaultStreamWriter(OperationExecutor opex,
                             @Assisted QueueName queueName,
                             @Assisted("accountId") String accountId,
                             @Assisted("applicationId") String applicationId) {
    this.opex = opex;
    this.opCtx = new OperationContext(accountId, applicationId);
    this.queueProducer = new QueueProducer("Testing");

    this.queueName = queueName;
    this.codec = new StreamEventCodec();
  }

  @Override
  public void send(String content) throws IOException {
    send(Charsets.UTF_8.encode(content));
  }

  @Override
  public void send(byte[] content) throws IOException {
    send(content, 0, content.length);
  }

  @Override
  public void send(byte[] content, int off, int len) throws IOException {
    send(ByteBuffer.wrap(content, off, len));
  }

  @Override
  public void send(ByteBuffer buffer) throws IOException {
    send(ImmutableMap.<String, String>of(), buffer);
  }

  @Override
  public void send(Map<String, String> headers, String content) throws IOException {
    send(headers, Charsets.UTF_8.encode(content));
  }

  @Override
  public void send(Map<String, String> headers, byte[] content) throws IOException {
    send(headers, content, 0, content.length);
  }

  @Override
  public void send(Map<String, String> headers, byte[] content, int off, int len) throws IOException {
    send(headers, ByteBuffer.wrap(content, off, len));
  }

  @Override
  public void send(Map<String, String> headers, ByteBuffer buffer) throws IOException {
    StreamEvent event = new DefaultStreamEvent(ImmutableMap.copyOf(headers), buffer);
    QueueEnqueue enqueue = new QueueEnqueue(queueProducer, queueName.toBytes(),
                                            new QueueEntry(codec.encodePayload(event)));
    try {
      opex.commit(opCtx, enqueue);
    } catch (OperationException e) {
      throw new IOException(e);
    }
  }
}
