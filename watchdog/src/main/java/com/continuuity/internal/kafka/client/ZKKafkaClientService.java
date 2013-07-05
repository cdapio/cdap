/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.kafka.client;

import com.continuuity.kafka.client.BrokerService;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.kafka.client.KafkaConsumer;
import com.continuuity.kafka.client.KafkaPublisher;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A KafkaClientService that uses ZooKeeper for broker discovery.
 */
public class ZKKafkaClientService extends AbstractService implements KafkaClientService, Runnable {

  private static final long PUBLISHER_CLEANUP_SECONDS = 1;

  private final BrokerService brokerService;

  // Maintains a weak reference key map for calling publisher.shutdown when garbage collected.
  private final Map<WeakReference<KafkaPublisher>, Cancellable> publishers;
  private final ReferenceQueue<KafkaPublisher> referenceQueue;

  private final SimpleKafkaConsumer consumer;

  // For running cleanup job
  private ScheduledExecutorService scheduler;

  public ZKKafkaClientService(ZKClient zkClient) {
    this.brokerService = new ZKBrokerService(zkClient);
    this.publishers = Collections.synchronizedMap(new IdentityHashMap<WeakReference<KafkaPublisher>, Cancellable>());
    this.referenceQueue = new ReferenceQueue<KafkaPublisher>();
    this.consumer = new SimpleKafkaConsumer(brokerService);
  }

  @Override
  public KafkaPublisher getPublisher(KafkaPublisher.Ack ack) {
    Preconditions.checkState(isRunning(), "Service is not running.");
    String brokerList = brokerService.getBrokerList();
    Preconditions.checkState(!brokerList.isEmpty(), "No broker available. Try again later.");

    // Wrap the publisher with a weak reference and save the cancellable for closing the publisher.
    SimpleKafkaPublisher publisher = new SimpleKafkaPublisher(brokerList, ack);
    publishers.put(new WeakReference<KafkaPublisher>(publisher, referenceQueue), publisher.start());
    return publisher;
  }

  @Override
  public KafkaConsumer getConsumer() {
    Preconditions.checkState(isRunning(), "Service is not running.");
    return consumer;
  }

  @Override
  protected void doStart() {
    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("kafka-client-cleanup"));
    scheduler.scheduleAtFixedRate(this, PUBLISHER_CLEANUP_SECONDS, PUBLISHER_CLEANUP_SECONDS, TimeUnit.SECONDS);

    // Start broker service to get auto-updated brokers information.
    Futures.addCallback(brokerService.start(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        notifyStarted();
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  @Override
  protected void doStop() {
    scheduler.shutdownNow();
    for (Cancellable cancellable : publishers.values()) {
      cancellable.cancel();
    }
    consumer.stop();

    Futures.addCallback(brokerService.stop(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        notifyStopped();
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void run() {
    // For calling publisher.producer.close() on garbage collected
    Reference<? extends KafkaPublisher> ref = referenceQueue.poll();
    while (ref != null && isRunning()) {
      publishers.remove(ref).cancel();
      ref = referenceQueue.poll();
    }
  }
}
