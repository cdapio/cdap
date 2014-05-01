/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.gateway.stream;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.stream.DefaultStreamEvent;
import com.continuuity.common.stream.StreamEventCodec;
import com.continuuity.test.StreamWriter;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class MultiThreadedStreamWriter implements StreamWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedStreamWriter.class);

  private final StreamEventCodec codec;


  private final List<com.continuuity.performance.gateway.stream.StreamEventDispatcher> dispatchers;
  private List<Future> dispatcherFutures;

  private final List<LinkedBlockingDeque<byte[]>> queues;

  private final AtomicInteger counter = new AtomicInteger(0);
  private final int numThreads;

  private final ExecutorService dispatcherThreadPool;

  @Inject
  public MultiThreadedStreamWriter(CConfiguration config, @Assisted QueueName queueName) {
    if (StringUtils.isNotEmpty(config.get("perf.dispatcher.threads"))) {
      numThreads = Integer.valueOf(config.get("perf.dispatcher.threads"));
    } else {
      numThreads = 1;
    }

    String gateway = config.get(Constants.AppFabric.SERVER_ADDRESS,
                                Constants.AppFabric.DEFAULT_SERVER_ADDRESS);
    if (StringUtils.isEmpty(gateway)) {
      gateway = "localhost";
    }
    String apiKey = config.get("apikey");
    Map<String, String> headers = null;
    if (!StringUtils.isEmpty(apiKey)) {
      headers = ImmutableMap.of(Constants.Gateway.CONTINUUITY_API_KEY, apiKey);
    }
    // todo
    String url = "Perf framework should be fixed towards new gateway";
      // Util.findBaseUrl(config, RestCollector.class, null, gateway, -1, apiKey != null)
      // + queueName.getSimpleName();

    dispatcherThreadPool = Executors.newFixedThreadPool(numThreads);
    queues = new ArrayList<LinkedBlockingDeque<byte[]>>(numThreads);
    dispatchers = new ArrayList<com.continuuity.performance.gateway.stream.StreamEventDispatcher>(numThreads);
    dispatcherFutures = new ArrayList<Future>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      LinkedBlockingDeque<byte[]> queue = new LinkedBlockingDeque<byte[]>(50000);
      queues.add(queue);
      com.continuuity.performance.gateway.stream.StreamEventDispatcher dispatcher =
        new StreamEventDispatcher(url, headers, queue);
      dispatchers.add(dispatcher);
      Future dispatcherFuture = dispatcherThreadPool.submit(dispatcher);
      dispatcherFutures.add(dispatcherFuture);
    }

    codec = new StreamEventCodec();
  }

  public void shutdown() {
    LOG.debug("Stopping all stream event dispatcher threads and the executor service...");
    for (int i = 0; i < numThreads; i++) {
      dispatchers.get(i).stop();
      dispatcherFutures.get(i).cancel(true);
    }
    dispatcherThreadPool.shutdown();
    LOG.debug("Stopped all stream event dispatcher threads and the executor service.");
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
    final int nextQueue = this.counter.getAndIncrement() % numThreads;
    queues.get(nextQueue).
      add(codec.encodePayload(new DefaultStreamEvent(ImmutableMap.copyOf(headers), buffer)));
    LOG.debug("Added stream event to dispatcher queue {}.", nextQueue);
  }
}
