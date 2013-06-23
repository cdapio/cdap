/**
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.gateway.Collector;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.streamevent.DefaultStreamEvent;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

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
   * The metrics object of the rest accessor.
   */
  private CMetrics metrics;

  /**
   * Constructor ensures that the collector is always set.
   *
   * @param collector the collector that this adapter belongs to
   */

  public FlumeAdapter(Collector collector) {
    this.collector = collector;
    this.metrics = collector.getMetricsClient();
  }

  /**
   * Get the collector that this adapter belongs to.
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
      // perform authentication of request
      if (!collector.getAuthenticator().authenticateRequest(event)) {
        helper.finish(Error);
        return Status.FAILED;
      }

      String accountId = collector.getAuthenticator().getAccountId(event);

      this.collector.getConsumer().consumeEvent(
        convertFlume2Event(event, helper, accountId), accountId);
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
      // perform authentication of request
      if (!collector.getAuthenticator().authenticateRequest(events.get(0))) {
        helper.finish(Error);
        return Status.FAILED;
      }

      String accountId = collector.getAuthenticator().getAccountId(events.get(0));

      this.collector.getConsumer().consumeEvents(
        convertFlume2Events(events, helper, accountId), accountId);
      helper.finish(Success);
      return Status.OK;
    } catch (Exception e) {
      LOG.warn("Error consuming batch of events: " + e.getMessage());
      helper.finish(Error);
      return Status.FAILED;
    }
  }

  /**
   * Converts a Flume event to a StreamEvent. This is a pure copy of the headers
   * and body. In addition, the collector name header is set.
   *
   * @param flumeEvent the flume event to be converted
   * @param helper     a metrics helper, if a destination is found,
   *                   the scope of this helper is updated to include it.
   * @param accountId  id of account used to send events
   * @return the resulting event
   */
  protected StreamEvent convertFlume2Event(AvroFlumeEvent flumeEvent,
                                           MetricsHelper helper, String accountId)
    throws Exception {

    // first construct the map of headers, just copy from flume event, plus:
    // - add the name of this collector
    // - drop the API key
    // - find the destination stream

    Map<String, String> headers = new TreeMap<String, String>();
    String destination = null;
    for (CharSequence header : flumeEvent.getHeaders().keySet()) {
      String headerKey = header.toString();
      if (!headerKey.equals(GatewayAuthenticator.CONTINUUITY_API_KEY)) {
        String headerValue = flumeEvent.getHeaders().get(header).toString();
        headers.put(headerKey, headerValue);
        if (headerKey.equals(Constants.HEADER_DESTINATION_STREAM)) {
          destination = headerValue;
        }
      }
    }
    headers.put(Constants.HEADER_FROM_COLLECTOR, this.getCollector().getName());

    if (destination == null) {
      throw new Exception("Cannot enqueue event without destination stream");
    }

    DefaultStreamEvent event = new DefaultStreamEvent(headers, flumeEvent.getBody());
    helper.setScope(destination);
    if (!this.collector.getStreamCache().validateStream(accountId, destination)) {
      helper.finish(NotFound);
      throw new Exception("Cannot enqueue event to non-existent stream '" +
                            destination + "'.");
    }
    return event;
  }

  /**
   * Converts a batch of Flume event to a lis of Events, using @ref
   * convertFlume2Event.
   *
   * @param flumeEvents the flume events to be converted
   * @param helper      a metrics helper, if a destination is found,
   *                    the scope of this helper is updated to include it.
   * @param accountId   id of account used to send events
   * @return the resulting events
   */
  protected List<StreamEvent> convertFlume2Events(List<AvroFlumeEvent> flumeEvents,
                                                  MetricsHelper helper, String accountId)
    throws Exception {
    List<StreamEvent> events = new ArrayList<StreamEvent>(flumeEvents.size());
    for (AvroFlumeEvent flumeEvent : flumeEvents) {
      events.add(convertFlume2Event(flumeEvent, helper, accountId));
    }
    return events;
  }
}
