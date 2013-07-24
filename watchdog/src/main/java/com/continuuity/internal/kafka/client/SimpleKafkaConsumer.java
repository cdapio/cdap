/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.kafka.client;

import com.continuuity.kafka.client.BrokerInfo;
import com.continuuity.kafka.client.BrokerService;
import com.continuuity.kafka.client.FetchedMessage;
import com.continuuity.kafka.client.KafkaConsumer;
import com.continuuity.kafka.client.TopicPartition;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.common.Threads;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetOutOfRangeException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link com.continuuity.kafka.client.KafkaConsumer} implementation using the scala kafka api.
 */
final class SimpleKafkaConsumer implements KafkaConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
  private static final int FETCH_SIZE = 1024 * 1024;        // Use a default fetch size.
  private static final int SO_TIMEOUT = 10 * 1000;          // 10 seconds.
  private static final long CONSUMER_EXPIRE_MINUTES = 1L;   // close consumer if not used for 1 minute.
  private static final int MAX_FAILURE_RETRY = 5;           // Maximum number of retries if there is error.
  private static final long FAILURE_RETRY_INTERVAL = 100L;  // Sleep for 100 ms before retry again.
  private static final long CONSUMER_FAILER_RETRY_INTERVAL = 5000L; // Sleep for 5 seconds if failure in consumer.
  private static final long EMPTY_FETCH_WAIT = 500L;        // Sleep for 500 ms if no message is fetched.

  private final BrokerService brokerService;
  private final LoadingCache<BrokerInfo, SimpleConsumer> consumers;
  private final BlockingQueue<Cancellable> consumerCancels;

  SimpleKafkaConsumer(BrokerService brokerService) {
    this.brokerService = brokerService;
    this.consumers = CacheBuilder.newBuilder()
                                 .expireAfterAccess(CONSUMER_EXPIRE_MINUTES, TimeUnit.MINUTES)
                                 .removalListener(createRemovalListener())
                                 .build(createConsumerLoader());
    this.consumerCancels = new LinkedBlockingQueue<Cancellable>();
  }

  @Override
  public Preparer prepare() {
    return new SimplePreparer();
  }

  /**
   * Called to stop all consumers created. This method should only be
   * called by KafkaClientService who own this consumer.
   */
  void stop() {
    List<Cancellable> cancels = Lists.newLinkedList();
    consumerCancels.drainTo(cancels);
    for (Cancellable cancel : cancels) {
      cancel.cancel();
    }
    consumers.invalidateAll();
  }

  /**
   * Creates a CacheLoader for creating SimpleConsumer.
   */
  private CacheLoader<BrokerInfo, SimpleConsumer> createConsumerLoader() {
    return new CacheLoader<BrokerInfo, SimpleConsumer>() {
      @Override
      public SimpleConsumer load(BrokerInfo key) throws Exception {
        return new SimpleConsumer(key.getHost(), key.getPort(), SO_TIMEOUT, FETCH_SIZE, "simple-kafka-client");
      }
    };
  }

  /**
   * Creates a RemovalListener that will close SimpleConsumer on cache removal.
   */
  private RemovalListener<BrokerInfo, SimpleConsumer> createRemovalListener() {
    return new RemovalListener<BrokerInfo, SimpleConsumer>() {
      @Override
      public void onRemoval(RemovalNotification<BrokerInfo, SimpleConsumer> notification) {
        SimpleConsumer consumer = notification.getValue();
        if (consumer != null) {
          consumer.close();
        }
      }
    };
  }

  /**
   * Picks any available broker.
   */
  private BrokerInfo pickBroker() {
    Iterator<BrokerInfo> itor = brokerService.getBrokers().iterator();
    Preconditions.checkState(itor.hasNext(), "No broker available.");
    return itor.next();
  }

  /**
   * Retrieves the last offset before the given timestamp for a given topic partition.
   *
   * @throws IllegalStateException if number of failure is greater than {@link #MAX_FAILURE_RETRY}.
   */
  private long getLastOffset(TopicPartition topicPartition, long timestamp) {
    return getLastOffset(topicPartition, timestamp, 1);
  }

  /**
   * Retrieves the last offset before the given timestamp for a given topic partition with a given trial count.
   *
   * @return the last offset that is earlier than the given timestamp.
   */
  private long getLastOffset(TopicPartition topicPart, long timestamp, int trial) {
    BrokerInfo brokerInfo = pickBroker();
    SimpleConsumer consumer = brokerInfo == null ? null : consumers.getUnchecked(brokerInfo);

    // If no broker, treat it as failure attempt.
    if (consumer == null) {
      if (trial <= MAX_FAILURE_RETRY) {
        LOG.info("No broker. Retry in {} ms.", FAILURE_RETRY_INTERVAL);
        Uninterruptibles.sleepUninterruptibly(FAILURE_RETRY_INTERVAL, TimeUnit.MILLISECONDS);
        return getLastOffset(topicPart, timestamp, trial + 1);
      }
      throw new IllegalStateException("Failed to talk to any broker.");
    }

    // Fire offset request
    OffsetRequest request = new OffsetRequest(ImmutableMap.of(
      new TopicAndPartition(topicPart.getTopic(), topicPart.getPartition()),
      new PartitionOffsetRequestInfo(timestamp, 1)
    ), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

    OffsetResponse response = consumer.getOffsetsBefore(request);

    // Retrieve offsets from response
    long[] offsets = response.hasError() ? null : response.offsets(topicPart.getTopic(), topicPart.getPartition());
    if (offsets == null || offsets.length <= 0) {
      short errorCode = response.errorCode(topicPart.getTopic(), topicPart.getPartition());
      if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
        // If the topic partition doesn't exists, use offset 0.
        return 0L;
      }

      consumers.refresh(brokerInfo);
      if (trial <= MAX_FAILURE_RETRY) {
        LOG.info("Failed to fetch offset for {} with timestamp {}. Error: {}", topicPart, timestamp, errorCode);
        Uninterruptibles.sleepUninterruptibly(FAILURE_RETRY_INTERVAL, TimeUnit.MILLISECONDS);
        return getLastOffset(topicPart, timestamp, trial + 1);
      }

      String errorMsg = String.format("Failed to fetch offset for %s with timestamp %d. Error: %d",
                                      topicPart, timestamp, errorCode);
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }

    LOG.debug("Offset {} fetched for {} with timestamp {} with {} attempts.",
              offsets[0], topicPart, timestamp, trial);

    return offsets[0];
  }

  /**
   * A preparer that uses kafak scala api for consuming messages.
   */
  private final class SimplePreparer implements Preparer {

    // Map from TopicPartition to offset
    private final Map<TopicPartition, Long> requests;
    private final ThreadFactory threadFactory;

    private SimplePreparer() {
      this.requests = Maps.newHashMap();
      this.threadFactory = Threads.createDaemonThreadFactory("message-callback-%d");
    }

    @Override
    public Preparer add(String topic, int partition, long offset) {
      requests.put(new TopicPartition(topic, partition), offset);
      return this;
    }

    @Override
    public Preparer addFromBeginning(String topic, int partition) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      requests.put(topicPartition, getLastOffset(topicPartition, kafka.api.OffsetRequest.EarliestTime()));
      return this;
    }

    @Override
    public Preparer addLatest(String topic, int partition) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      requests.put(topicPartition, getLastOffset(topicPartition, kafka.api.OffsetRequest.LatestTime()));
      return this;
    }

    @Override
    public Cancellable consume(MessageCallback callback) {
      final ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
      final List<ConsumerThread> pollers = Lists.newArrayList();

      // When cancelling the consumption, first terminates all polling threads and then stop the executor service.
      final AtomicBoolean cancelled = new AtomicBoolean();
      Cancellable cancellable = new Cancellable() {
        @Override
        public void cancel() {
          if (!cancelled.compareAndSet(false, true)) {
            return;
          }
          consumerCancels.remove(this);

          LOG.info("Requesting stop of all consumer threads.");
          for (ConsumerThread consumerThread : pollers) {
            consumerThread.terminate();
          }
          LOG.info("Wait for all consumer threads to stop.");
          for (ConsumerThread consumerThread : pollers) {
            try {
              consumerThread.join();
            } catch (InterruptedException e) {
              LOG.warn("Interrupted exception while waiting for thread to complete.", e);
            }
          }
          LOG.info("All consumer threads stopped.");
          // Use shutdown so that submitted task still has chance to execute, which is important for finished to get
          // called.
          executor.shutdown();
        }
      };

      // Wrap the callback with a single thread executor.
      MessageCallback messageCallback = wrapCallback(callback, executor, cancellable);

      // Starts threads for polling new messages.
      for (Map.Entry<TopicPartition, Long> entry : requests.entrySet()) {
        ConsumerThread consumerThread = new ConsumerThread(entry.getKey(), entry.getValue(), messageCallback);
        consumerThread.setDaemon(true);
        consumerThread.start();
        pollers.add(consumerThread);
      }

      consumerCancels.add(cancellable);
      return cancellable;
    }

    /**
     * Wrap a given MessageCallback by a executor so that calls are executed in the given executor.
     * By running the calls through the executor, it also block and wait for the task being completed so that
     * it can block the poller thread depending on the rate of processing that the callback can handle.
     */
    private MessageCallback wrapCallback(final MessageCallback callback,
                                         final ExecutorService executor, final Cancellable cancellable) {
      final AtomicBoolean stopped = new AtomicBoolean();
      return new MessageCallback() {
        @Override
        public void onReceived(final Iterator<FetchedMessage> messages) {
          if (stopped.get()) {
            return;
          }
          Futures.getUnchecked(executor.submit(new Runnable() {
            @Override
            public void run() {
              if (stopped.get()) {
                return;
              }
              callback.onReceived(messages);
            }
          }));
        }

        @Override
        public void finished(final boolean error, final Throwable cause) {
          // Make sure finished only get called once.
          if (!stopped.compareAndSet(false, true)) {
            return;
          }
          Futures.getUnchecked(executor.submit(new Runnable() {
            @Override
            public void run() {
              // When finished is called, also cancel the consumption from all poller thread.
              callback.finished(error, cause);
              cancellable.cancel();
            }
          }));
        }
      };
    }
  }

  /**
   * The thread for polling kafka.
   */
  private final class ConsumerThread extends Thread {

    private final TopicPartition topicPart;
    private final long startOffset;
    private final MessageCallback callback;
    private final BasicFetchedMessage fetchedMessage;
    private volatile boolean running;

    private ConsumerThread(TopicPartition topicPart, long startOffset, MessageCallback callback) {
      super(String.format("Kafka-Consumer-%s-%d", topicPart.getTopic(), topicPart.getPartition()));
      this.topicPart = topicPart;
      this.startOffset = startOffset;
      this.callback = callback;
      this.running = true;
      this.fetchedMessage = new BasicFetchedMessage(topicPart);
    }

    @Override
    public void run() {
      final AtomicLong offset = new AtomicLong(startOffset);

      Map.Entry<BrokerInfo, SimpleConsumer> consumerEntry = null;
      Throwable errorCause = null;

      while (running) {
        if (consumerEntry == null && (consumerEntry = getConsumerEntry()) == null) {
          try {
            TimeUnit.MICROSECONDS.sleep(CONSUMER_FAILER_RETRY_INTERVAL);
          } catch (InterruptedException e) {
            // OK to ignore this, as interrupt would be caused by thread termination.
            LOG.debug("Consumer sleep interrupted.", e);
          }
          continue;
        }

        SimpleConsumer consumer = consumerEntry.getValue();

        // Fire a fetch message request
        FetchResponse response = fetchMessages(consumer, offset.get());

        // Failure response, set consumer entry to null and let next round of loop to handle it.
        if (response.hasError()) {
          short errorCode = response.errorCode(topicPart.getTopic(), topicPart.getPartition());
          LOG.info("Failed to fetch message on {}. Error: {}", topicPart, errorCode);
          // If it is out of range error, throw an stop the callback with an exception.
          if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
            errorCause = new OffsetOutOfRangeException("Offset out of range: " + offset.get());
            break;
          }

          consumers.refresh(consumerEntry.getKey());
          consumerEntry = null;
          continue;
        }

        ByteBufferMessageSet messages = response.messageSet(topicPart.getTopic(), topicPart.getPartition());
        if (sleepIfEmpty(messages)) {
          continue;
        }

        // Call the callback
        invokeCallback(messages, offset);
      }

      // When the thread is done, call the callback finished method.
      try {
        callback.finished(running, errorCause);
      } catch (Throwable t) {
        LOG.error("Exception thrown from MessageCallback.finished({})", running, t);
      }
    }

    public void terminate() {
      LOG.info("Terminate requested {}", getName());
      running = false;
      interrupt();
    }

    /**
     * Gets the leader broker and the associated SimpleConsumer for the current topic and partition.
     */
    private Map.Entry<BrokerInfo, SimpleConsumer> getConsumerEntry() {
      BrokerInfo leader = brokerService.getLeader(topicPart.getTopic(), topicPart.getPartition());
      return leader == null ? null : Maps.immutableEntry(leader, consumers.getUnchecked(leader));
    }

    /**
     * Makes a call to kafka to fetch messages.
     */
    private FetchResponse fetchMessages(SimpleConsumer consumer, long offset) {
      FetchRequest request = new FetchRequestBuilder()
        .clientId(consumer.clientId())
        .addFetch(topicPart.getTopic(), topicPart.getPartition(), offset, FETCH_SIZE)
        .maxWait(1000)
        .build();
      return consumer.fetch(request);
    }

    /**
     * Sleeps if the message set is empty.
     * @return {@code true} if it is empty, {@code false} otherwise.
     */
    private boolean sleepIfEmpty(ByteBufferMessageSet messages) {
      if (Iterables.isEmpty(messages)) {
        LOG.trace("No message fetched. Sleep for {} ms before next fetch.", EMPTY_FETCH_WAIT);
        try {
          TimeUnit.MILLISECONDS.sleep(EMPTY_FETCH_WAIT);
        } catch (InterruptedException e) {
          // It's interrupted from stop, ok to ignore.
        }
        return true;
      }
      return false;
    }

    /**
     * Calls the message callback with the given message set.
     */
    private void invokeCallback(ByteBufferMessageSet messages, AtomicLong offset) {
      long savedOffset = offset.get();
      try {
        callback.onReceived(createFetchedMessages(messages, offset));
      } catch (Throwable t) {
        LOG.error("Callback throws exception. Retry from offset {} for {}", startOffset, topicPart, t);
        offset.set(savedOffset);
      }
    }

    /**
     * Creates an Iterator of FetchedMessage based on the given message set. The iterator would also updates
     * the offset while iterating.
     */
    private Iterator<FetchedMessage> createFetchedMessages(ByteBufferMessageSet messageSet, final AtomicLong offset) {
      final Iterator<MessageAndOffset> messages = messageSet.iterator();
      return new AbstractIterator<FetchedMessage>() {
        @Override
        protected FetchedMessage computeNext() {
          while (messages.hasNext()) {
            MessageAndOffset message = messages.next();
            long msgOffset = message.offset();
            if (msgOffset < offset.get()) {
              LOG.trace("Received old offset {}, expecting {} on {}. Message Ignored.",
                        msgOffset, offset.get(), topicPart);
              continue;
            }

            offset.set(message.nextOffset());
            fetchedMessage.setPayload(message.message().payload());
            fetchedMessage.setNextOffset(offset.get());

            return fetchedMessage;
          }
          return endOfData();
        }
      };
    }
  }
}
