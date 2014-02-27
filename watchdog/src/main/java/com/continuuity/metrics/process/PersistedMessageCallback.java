package com.continuuity.metrics.process;

import com.continuuity.data2.OperationException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link KafkaConsumer.MessageCallback} that persists offset information into a VCTable while
 * delegating the actual message consumption to another {@link KafkaConsumer.MessageCallback}.
 */
public final class PersistedMessageCallback implements KafkaConsumer.MessageCallback {

  private static final Logger LOG = LoggerFactory.getLogger(PersistedMessageCallback.class);

  private final KafkaConsumer.MessageCallback delegate;
  private final KafkaConsumerMetaTable metaTable;
  private final int persistThreshold;
  private final Map<TopicPartition, Long> offsets;
  private final AtomicInteger messageCount;

  /**
   * Constructs a {@link PersistedMessageCallback} which delegates to the given callback for actual action while
   * persisting offsets information in to the given meta table when number of messages has been processed based
   * on the persistThreshold.
   */
  public PersistedMessageCallback(KafkaConsumer.MessageCallback delegate,
                                  KafkaConsumerMetaTable metaTable,
                                  int persistThreshold) {
    this.delegate = delegate;
    this.metaTable = metaTable;
    this.persistThreshold = persistThreshold;
    this.offsets = Maps.newConcurrentMap();
    this.messageCount = new AtomicInteger();
  }

  @Override
  public void onReceived(Iterator<FetchedMessage> messages) {
    delegate.onReceived(new OffsetTrackingIterator(messages));
  }

  @Override
  public void finished() {
    try {
      delegate.finished();
    } finally {
      // Save the offset
      persist();
    }
  }

  private void persist() {
    try {
      metaTable.save(ImmutableMap.copyOf(offsets));
    } catch (OperationException e) {
      // Simple log and ignore the error.
      LOG.error("Failed to persist consumed message offset. {}", e.getMessage(), e);
    }
  }

  /**
   * Inner help class to track offsets of {@link FetchedMessage} being consumed and
   * persist to meta table when persistThreshold is reached.
   */
  private final class OffsetTrackingIterator implements Iterator<FetchedMessage> {

    private final Iterator<FetchedMessage> delegate;
    private TopicPartition lastTopicPartition;
    private long lastOffset = -1;

    OffsetTrackingIterator(Iterator<FetchedMessage> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public FetchedMessage next() {
      recordLastOffset();
      FetchedMessage message = delegate.next();
      lastTopicPartition = message.getTopicPartition();
      lastOffset = message.getNextOffset();

      messageCount.incrementAndGet();
      return message;
    }

    @Override
    public void remove() {
      delegate.remove();
    }

    private void recordLastOffset() {
      if (lastOffset >= 0) {
        offsets.put(lastTopicPartition, lastOffset);
      }
      if (messageCount.get() >= persistThreshold) {
        messageCount.set(0);
        persist();
      }
    }
  }
}
