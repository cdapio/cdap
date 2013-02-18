/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector;

import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.flow.flowlet.internal.EventBuilder;
import com.continuuity.gateway.Collector;
import com.continuuity.gateway.Constants;

/**
 * /**
 * This class is the intermediary between the Flume Avro receiver (Flume
 * events come in through Avro RPC) and the consumer that persists the
 * events into a stream/queue. In particular, it is responsible for
 * <ul>
 * <li>Mapping a flume event to a flow event, including the filtering and
 * mapping and adding of headers</li>
 * <li>Depending on the success of the consumer, create an Avro response
 * for the client (the Avro sink in a customer's Flume flow) to indicate
 * success or failure of the ingestion of the event.
 * </li>
 * </ul>
 */
class FlumeAdapter implements AvroSourceProtocol {

  private static final Logger LOG = LoggerFactory
      .getLogger(FlumeAdapter.class);

  /**
   * The collector that created this handler. It has collector name and consumer
   */
  private Collector collector;

  /**
   * The metrics object of the rest accessor
   */
  private CMetrics metrics;

  /**
   * Constructor ensures that the collector is always set
   *
   * @param collector the collector that this adapter belongs to
   */

  public FlumeAdapter(Collector collector) {
    this.collector = collector;
    this.metrics = collector.getMetricsClient();
  }

  /**
   * Get the collector that this adapter belongs to
   *
   * @return the collector
   */
  public Collector getCollector() {
    return this.collector;
  }

  @Override
  /** called by the Avro Responder for each single event */
  public final Status append(AvroFlumeEvent event) {
    MetricsHelper helper = new MetricsHelper(this.getClass(),
        this.metrics, this.collector.getMetricsQualifier(), "append");
    LOG.trace("Received event: " + event);
    try {
      Event convertedEvent = convertFlume2Event(event, helper);
      // perform authentication of request
      if (!collector.getAuthenticator().authenticateRequest(convertedEvent)) {
        LOG.debug("Received an unauthorized request");
        helper.finish(Error);
        return Status.FAILED;
      }
      this.collector.getConsumer().consumeEvent(convertedEvent);
      helper.finish(MetricsHelper.Status.Success);
      return Status.OK;
    } catch (Exception e) {
      LOG.warn("Error consuming single event: " + e.getMessage());
      helper.finish(Error);
      return Status.FAILED;
    }
  }

  @Override
  /** called by the Avro Responder for each batch of events */
  public final Status appendBatch(List<AvroFlumeEvent> events) {
    MetricsHelper helper = new MetricsHelper(this.getClass(),
        this.metrics, this.collector.getMetricsQualifier(), "batch");
    LOG.trace("Received batch: " + events);
    try {
      List<Event> convertedEvents = convertFlume2Event(events, helper);
      // perform authentication of request
      if (!collector.getAuthenticator().authenticateRequest(
          convertedEvents.get(0))) {
        LOG.warn("Received an unauthorized request");
        helper.finish(Error);
        return Status.FAILED;
      }
      this.collector.getConsumer().consumeEvents(convertedEvents);
      helper.finish(Success);
      return Status.OK;
    } catch (Exception e) {
      LOG.warn("Error consuming batch of events: " + e.getMessage());
      helper.finish(Error);
      return Status.FAILED;
    }
  }

  /**
   * Converts a Flume event to am Event. This is a pure copy of the headers
   * and body. In addition, the collector name header is set.
   *
   * @param flumeEvent the flume event to be converted
   * @param helper a metrics helper, if a destination is found,
   *               the scope of this helper is updated to include it.
   * @return the resulting event
   */
  protected Event convertFlume2Event(AvroFlumeEvent flumeEvent,
                                     MetricsHelper helper)
      throws Exception {
    EventBuilder builder = new EventBuilder();
    builder.setBody(flumeEvent.getBody().array());
    for (CharSequence header : flumeEvent.getHeaders().keySet()) {
      builder.setHeader(header.toString(),
          flumeEvent.getHeaders().get(header).toString());
    }
    builder.setHeader(Constants.HEADER_FROM_COLLECTOR,
        this.getCollector().getName());
    Event event = builder.create();
    String destination = event.getHeader(Constants.HEADER_DESTINATION_STREAM);
    if (destination == null) {
      throw new Exception("Cannot enqueue event without destination stream");
    }
    helper.setScope(destination);
    if (!this.collector.getStreamCache().validateStream(
        OperationContext.DEFAULT_ACCOUNT_ID, destination)) {
      helper.finish(NotFound);
      throw new Exception("Cannot enqueue event to non-existent stream '" +
          destination + "'.");
    }
    return event;
  }

  /**
   * Converts a batch of Flume event to a lis of Events, using @ref
   * convertFlume2Event
   *
   * @param flumeEvents the flume events to be converted
   * @param helper a metrics helper, if a destination is found,
   *               the scope of this helper is updated to include it.
   * @return the resulting events
   */
  protected List<Event> convertFlume2Event(List<AvroFlumeEvent> flumeEvents,
                                           MetricsHelper helper)
  throws Exception {
    List<Event> events = new ArrayList<Event>();
    for (AvroFlumeEvent flumeEvent : flumeEvents) {
      events.add(convertFlume2Event(flumeEvent, helper));
    }
    return events;
  }
}
