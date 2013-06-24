package com.continuuity.performance.gateway.stream;

import com.continuuity.performance.gateway.SimpleHttpClient;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dispatcher for sending stream events to the Continuuity Gateway. It runs in a seperate thread dequeuing the events
 * written by a stream writer.
 */
public final class StreamEventDispatcher implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamEventDispatcher.class);

  private final AtomicLong counter = new AtomicLong(0);
  private final SimpleHttpClient httpClient;
  private final LinkedBlockingDeque<byte[]> queue;

  private volatile boolean keepRunning = true;

  /**
   * Constructs and initializes {@link StreamEventDispatcher}.
   */
  public StreamEventDispatcher(String url, Map<String, String> headers, final LinkedBlockingDeque<byte[]> queue) {
    this.queue = queue;
    httpClient = new SimpleHttpClient(url, headers);
  }

  /**
   * Stops the running thread.
   */
  public void stop() {
    keepRunning = false;
  }

  @Override
  public void run() {
    // While we are not asked to stop and queue is not empty, we keep on going.
    while (keepRunning) {
      // We pop the stream event to be send from the queue and then attempt to send it over.
      final byte[] event;
      try {
        // blocking call will wait till there is an element in the queue.
        event = queue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.debug("Stream event dispatcher thread has been interrupted.");
        continue;
      }

      // Make sure we have not received a null object. This is just a precaution.
      if (event == null) {
        continue;
      }

      LOG.debug("Trying to send stream event {} to gateway.", event);

      try {
        httpClient.post(event);
        LOG.debug("Successfully sent {} stream events to the gateway.", counter.incrementAndGet());
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
    LOG.debug("Stream event dispatcher finished.");
  }
}