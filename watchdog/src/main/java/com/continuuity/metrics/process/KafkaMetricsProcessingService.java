package com.continuuity.metrics.process;

import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.weave.common.Threads;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Service for processing metrics by consuming metrics being published to kafka.
 */
public final class KafkaMetricsProcessingService extends AbstractService {

  private final KafkaClientService kafkaClient;
  private final String topic;
  private final int partitionSize;
  private final int threadPoolSize;
  private ExecutorService executor;

  @Inject
  public KafkaMetricsProcessingService(KafkaClientService kafkaClient,
                                       @Named(MetricsConstants.ConfigKeys.KAFKA_TOPIC) String topic,
                                       @Named(MetricsConstants.ConfigKeys.KAFKA_PARTITION_SIZE) int partitionSize,
                                       @Named(MetricsConstants.ConfigKeys.PROCESSING_THREADS) int threadPoolSize) {
    this.kafkaClient = kafkaClient;
    this.topic = topic;
    this.partitionSize = partitionSize;
    this.threadPoolSize = threadPoolSize;
  }

  @Override
  protected void doStart() {
    try {
      executor = Executors.newFixedThreadPool(threadPoolSize, Threads.createDaemonThreadFactory("metrics-process-%d"));


      notifyStarted();
    } catch (Throwable t) {
      notifyFailed(t);
    }
  }

  @Override
  protected void doStop() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  private Runnable createPoller(int thread) {
    return new Runnable() {

      @Override
      public void run() {
//        kafkaClient.getConsumer().prepare().
      }
    };
  }
}
