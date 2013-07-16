package com.continuuity.metrics.process;

import com.continuuity.api.data.OperationException;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.kafka.client.KafkaConsumer;
import com.continuuity.kafka.client.TopicPartition;
import com.continuuity.metrics.MetricsConstants.ConfigKeys;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.common.Threads;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Service for processing metrics by consuming metrics being published to kafka.
 */
public final class KafkaMetricsProcessingService extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsProcessingService.class);

  private final KafkaClientService kafkaClient;
  private final KafkaConsumerMetaTable metaTable;
  private final MessageCallbackFactory callbackFactory;
  private final String topic;
  private final int partitionSize;
  private final int threadPoolSize;
  private ExecutorService executor;

  @Inject
  public KafkaMetricsProcessingService(KafkaClientService kafkaClient,
                                       KafkaConsumerMetaTable metaTable,
                                       MessageCallbackFactory callbackFactory,
                                       @Named(ConfigKeys.KAFKA_TOPIC) String topic,
                                       @Named(ConfigKeys.KAFKA_PARTITION_SIZE) int partitionSize,
                                       @Named(ConfigKeys.PROCESSING_THREADS) int threadPoolSize) {
    this.kafkaClient = kafkaClient;
    this.metaTable = metaTable;
    this.callbackFactory = callbackFactory;
    this.topic = topic;
    this.partitionSize = partitionSize;
    this.threadPoolSize = threadPoolSize;
  }

  @Override
  protected void doStart() {
    try {
      executor = Executors.newFixedThreadPool(threadPoolSize, Threads.createDaemonThreadFactory("metrics-process-%d"));
      int size = (partitionSize < threadPoolSize) ? 1 : Math.round((float) partitionSize / threadPoolSize);
      for (int i = 0; i < threadPoolSize; i++) {
        executor.submit(createPoller(i * size, (i + 1) * size + 1));
      }
      notifyStarted();
    } catch (Throwable t) {
      notifyFailed(t);
    }
  }

  @Override
  protected void doStop() {
    executor.shutdownNow();
    notifyStopped();
  }

  private Runnable createPoller(final int startPartition, final int endPartition) {
    return new Runnable() {

      @Override
      public void run() {
        // Assuming there is only one process that pulling in all metrics.
        KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

        for (int i = startPartition; i < endPartition && i < partitionSize; i++) {
          long offset = getOffset(topic, i);
          if (offset >= 0) {
            preparer.add(topic, i, offset);
          } else {
            preparer.addFromBeginning(topic, i);
          }
        }

        LOG.info("Consumer created for topic {} for partitions from {} to {}", topic, startPartition, endPartition - 1);
        Cancellable cancel = preparer.consume(callbackFactory.create());
        CountDownLatch latch = new CountDownLatch(1);
        try {
          latch.await();
        } catch (InterruptedException e) {
          LOG.info("Thread interrupted for shutdown.");
        }
        cancel.cancel();
      }
    };
  }

  private long getOffset(String topic, int partition) {
    try {
      return metaTable.get(new TopicPartition(topic, partition));
    } catch (OperationException e) {
      LOG.error("Failed to get offset from meta table. Defaulting to beginning. {}", e.getMessage(), e);
    }
    return -1;
  }
}
