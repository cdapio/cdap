package com.continuuity.test.internal;

import com.continuuity.data2.OperationException;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.continuuity.test.StreamWriter;
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

  private final Queue2Producer producer;
  private final TransactionSystemClient txSystemClient;
  private final QueueName queueName;
  private final StreamEventCodec codec;

  @Inject
  public DefaultStreamWriter(QueueClientFactory queueClientFactory,
                             TransactionSystemClient txSystemClient,
                             @Assisted QueueName queueName,
                             @Assisted("accountId") String accountId,
                             @Assisted("applicationId") String applicationId) throws IOException {

    this.producer = queueClientFactory.createProducer(queueName);
    this.txSystemClient = txSystemClient;
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
    TransactionAware txAware = (TransactionAware) producer;

    // start tx to write in queue in tx
    Transaction tx = txSystemClient.startShort();
    txAware.startTx(tx);
    try {
      producer.enqueue(new QueueEntry(codec.encodePayload(event)));
      if (!txSystemClient.canCommit(tx, txAware.getTxChanges()) || !txAware.commitTx() || !txSystemClient.commit(tx)) {
        throw new OperationException(StatusCode.TRANSACTION_CONFLICT, "Fail to commit");
      }
      txAware.postTxCommit();
    } catch (Exception e) {
      try {
        txAware.rollbackTx();
        txSystemClient.abort(tx);
      } catch (Exception ex) {
        throw new IOException(ex);
      }
      throw new IOException(e);
    }
  }
}
