/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The consumer is responsible for the actual ingestion of an event
 * or a batch of events received by a collector. For this it needs
 * to provide abstract methods single() and batch(), and in addition
 * to methods start() and stop() to - guess what - start and stop
 * the consumer. These methods are protected and cannot be called
 * directly by the collector.
 * <p/>
 * Instead, this base class for all consumers wraps the four
 * abstract methods into four methods start/stopConsumer and
 * consumeEvent/Events, which we can use to inject code into
 * every consumer (these methods are final and cannot be overridden
 * by implementing classes).
 * <p/>
 * At this point, all we do here is count the number of events and calls
 * that were received, processed successfully, or failed to process.
 */
public abstract class Consumer {

  private final Logger LOG = LoggerFactory.getLogger(this.getClass());

  /**
   * the number of calls that were received since the consumer started.
   */
  private AtomicLong callsReceived = new AtomicLong();
  /**
   * the number of calls that succeeded since the consumer started.
   */
  private AtomicLong callsSucceeded = new AtomicLong();
  /**
   * the number of calls that failed since the consumer started.
   */
  private AtomicLong callsFailed = new AtomicLong();
  /**
   * the number of events that were received since the consumer started.
   */
  private AtomicLong eventsReceived = new AtomicLong();
  /**
   * the number of events that were successfully consumed since the consumer
   * started.
   */
  private AtomicLong eventsSucceeded = new AtomicLong();
  /**
   * the number of events that failed to be consumed since the consumer started.
   */
  private AtomicLong eventsFailed = new AtomicLong();

  /**
   * @return the number of calls that were received since the consumer started.
   */
  public long callsReceived() {
    return this.callsReceived.get();
  }

  /**
   * @return the number of calls that succeeded since the consumer started.
   */
  public long callsSucceeded() {
    return this.callsSucceeded.get();
  }

  /**
   * @return the number of calls that failed since the consumer started
   */
  public long callsFailed() {
    return this.callsFailed.get();
  }

  /**
   * @return the number of events that were received since the consumer started
   */
  public long eventsReceived() {
    return this.eventsReceived.get();
  }

  /**
   * @return the number of events that were successfully consumed since the
   *         consumer started
   */
  public long eventsSucceeded() {
    return this.eventsSucceeded.get();
  }

  /**
   * @return the number of events that failed to be consumed since the
   *         consumer started
   */
  public long eventsFailed() {
    return this.eventsFailed.get();
  }

  /**
   * Configure the consumer. By default does nothing.
   *
   * @param configuration The configuration that has all the options
   */
  public void configure(@SuppressWarnings("unused") CConfiguration configuration) {
  }

  /**
   * Start the consumer. This is where any initialization of state happens.
   * By default does nothing.
   */
  protected void start() {
  }

  /**
   * Start the consumer. This is where de-initialization of state happens.
   * By default does nothing.
   */
  protected void stop() {
  }

  /**
   * Consume a single event. This method is abstract and must be overridden
   *
   * @param event     the event to be consumed
   * @param accountId id of account used to send events
   * @throws Exception if anything goes wrong
   */
  protected abstract void single(StreamEvent event, String accountId) throws Exception;

  /**
   * Consume a batch of events. By default calls single() for every event in
   * the batch.
   *
   * @param events    the batch of events to be consumed
   * @param accountId id of account used to send events
   * @throws Exception if anything goes wrong
   */
  protected void batch(List<StreamEvent> events, String accountId) throws Exception {
    for (StreamEvent event : events) {
      this.single(event, accountId);
    }
  }

  /**
   * Start the consumer. This is called by the collector/gateway that owns the
   * consumer, and itself calls start() which can be overridden by subclasses.
   */
  final void startConsumer() {
    LOG.info("Consumer Starting up.");

    this.callsReceived.set(0L);
    this.callsSucceeded.set(0L);
    this.callsFailed.set(0L);
    this.eventsReceived.set(0L);
    this.eventsSucceeded.set(0L);
    this.eventsFailed.set(0L);

    this.start();
  }

  /**
   * Start the consumer. This is called by the collector/gateway that owns the
   * consumer, and itself calls stop() which can be overridden by subclasses.
   */
  final void stopConsumer() {
    this.stop();
    LOG.info("Consumer Shutting down.");
    LOG.info("  Calls/Events Received : " +
               this.callsReceived + "/" + this.eventsReceived);
    LOG.info("  Calls/Events Succeeded: " +
               this.callsSucceeded + "/" + this.eventsSucceeded);
    LOG.info("  Calls/Events Failed:    " +
               this.callsFailed + "/" + this.eventsFailed);
  }

  /**
   * Consume one event. This is called by the collector/gateway that owns the
   * consumer. It does some counting and then calls single() which can be
   * overridden by subclasses.
   *
   * @param event     The event to be consumed
   * @param accountId id of account used to send events
   * @throws Exception if anything goes wrong
   */
  public final void consumeEvent(StreamEvent event, String accountId) throws Exception {
    this.callsReceived.incrementAndGet();
    this.eventsReceived.incrementAndGet();
    try {
      this.single(event, accountId);
    } catch (Exception e) {
      this.callsFailed.incrementAndGet();
      this.eventsFailed.incrementAndGet();
      throw e;
    }
    this.callsSucceeded.incrementAndGet();
    this.eventsSucceeded.incrementAndGet();
  }

  /**
   * Consume a batch of event. This is called by the collector/gateway that
   * owns the consumer. It does some counting and then calls batch() which
   * can be overridden by subclasses.
   *
   * @param events    The events to be consumed
   * @param accountId id of account used to send events
   * @throws Exception if anything goes wrong
   */
  public final void consumeEvents(List<StreamEvent> events, String accountId) throws Exception {
    this.callsReceived.incrementAndGet();
    this.eventsReceived.addAndGet(events.size());
    try {
      this.batch(events, accountId);
    } catch (Exception e) {
      this.callsFailed.incrementAndGet();
      this.eventsFailed.addAndGet(events.size());
      throw e;
    }
    this.callsSucceeded.incrementAndGet();
    this.eventsSucceeded.addAndGet(events.size());
  }

}
