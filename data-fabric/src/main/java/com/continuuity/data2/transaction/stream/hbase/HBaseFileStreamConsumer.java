/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.hbase;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.data.file.FileReader;
import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StreamConsumer} that read events from stream file and uses HBase to store consumer states.
 */
@NotThreadSafe
public final class HBaseFileStreamConsumer implements StreamConsumer {

  private final StreamConfig streamConfig;
  private final ConsumerConfig consumerConfig;
  private final HTable hTable;
  private final FileReader<StreamEvent, Iterable<StreamFileOffset>> reader;
  private final List<StreamEvent> events;
  private Transaction transaction;

  /**
   *
   * @param streamConfig
   * @param consumerConfig
   * @param hTable For communicate with HBase for storing polled entry states (not consumer state). This class is
   *               responsible for closing the HTable.
   */
  public HBaseFileStreamConsumer(StreamConfig streamConfig, ConsumerConfig consumerConfig, HTable hTable) {
    this.streamConfig = streamConfig;
    this.consumerConfig = consumerConfig;
    this.hTable = hTable;
    this.events = Lists.newArrayList();
    this.reader = null;
  }

  @Override
  public String getStreamName() {
    return streamConfig.getName();
  }

  @Override
  public ConsumerConfig getConsumerConfig() {
    return consumerConfig;
  }

  @Override
  public StreamConsumer.Result poll(int maxEvents, long timeout,
                                    TimeUnit timeoutUnit) throws IOException, InterruptedException {



    int eventCount = reader.read(events, maxEvents, timeout, timeoutUnit);
    if (eventCount == 0) {
      return EMPTY_RESULT;
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    try {
      hTable.close();
    } finally {
      reader.close();
    }
  }

  @Override
  public void startTx(Transaction tx) {
    transaction = tx;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    // Guaranteed no conflict in the consumer logic
    return ImmutableList.of();
  }

  @Override
  public boolean commitTx() throws Exception {
    return false;
  }

  @Override
  public void postTxCommit() {

  }

  @Override
  public boolean rollbackTx() throws Exception {
    return false;
  }

  @Override
  public String getName() {
    return toString();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("stream", streamConfig)
      .add("consumer", consumerConfig)
      .toString();
  }

  private static final class SimpleResult implements Result {

    private final List<StreamEvent> events;

    private SimpleResult(List<StreamEvent> events) {
      this.events = Collections.unmodifiableList(events);
    }

    @Override
    public boolean isEmpty() {
      return events.isEmpty();
    }

    @Override
    public int size() {
      return events.size();
    }

    @Override
    public Iterator<StreamEvent> iterator() {
      return events.iterator();
    }
  }
}
