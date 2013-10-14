/**
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.StreamCache;
import com.continuuity.gateway.v2.handlers.v2.stream.CachedStreamEventCollector;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.inject.Inject;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.CallFuture;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

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
class FlumeAdapter extends AbstractIdleService implements AvroSourceProtocol.Callback {

  private static final Logger LOG = LoggerFactory.getLogger(FlumeAdapter.class);

  private final StreamCache streamCache;
  private final CachedStreamEventCollector streamEventCollector;
  private final GatewayAuthenticator authenticator;
  private final DiscoveryServiceClient discoveryClient;

  @Inject
  FlumeAdapter(StreamCache streamCache, CachedStreamEventCollector streamEventCollector,
               GatewayAuthenticator authenticator, DiscoveryServiceClient discoveryClient) {
    this.streamCache = streamCache;
    this.streamEventCollector = streamEventCollector;
    this.authenticator = authenticator;
    this.discoveryClient = discoveryClient;
  }

  @Override
  protected void startUp() throws Exception {
    EndpointStrategy endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(
        com.continuuity.common.conf.Constants.Service.APP_FABRIC)), 1L, TimeUnit.SECONDS);
    this.streamCache.init(endpointStrategy);
    streamEventCollector.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    streamEventCollector.stopAndWait();
  }

  @Override
  public void append(AvroFlumeEvent event, final org.apache.avro.ipc.Callback<Status> callback) throws IOException {
    try {
      // perform authentication of request
      if (!authenticator.authenticateRequest(event)) {
        LOG.trace("Failed authentication for stream append");
        callback.handleResult(Status.FAILED);
      }

      final String accountId = authenticator.getAccountId(event);

      streamEventCollector.collect(convertFlume2Event(event, accountId), accountId, new FutureCallback<Void>() {
        @Override
        public void onSuccess(Void result) {
          callback.handleResult(Status.OK);
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.error("Error consuming single event", t);
          callback.handleError(t);
        }
      });
    } catch (Exception e) {
      LOG.error("Error consuming single event", e);
      callback.handleError(e);
    }
  }

  @Override
  public void appendBatch(List<AvroFlumeEvent> events, final org.apache.avro.ipc.Callback<Status> callback)
    throws IOException {
    try {
      // perform authentication of request
      if (!authenticator.authenticateRequest(events.get(0))) {
        callback.handleResult(Status.FAILED);
      }

      if (events.isEmpty()) {
        callback.handleResult(Status.OK);
      }

      String accountId = authenticator.getAccountId(events.get(0));
      List<StreamEvent> streamEvents = convertFlume2Events(events, accountId);

      // Send a no-op callback for the first n-1 events
      int size = streamEvents.size();
      for (int i = 0; i < size - 1; ++i) {
        streamEventCollector.collect(streamEvents.get(i), accountId, NO_OP_CALLBACK);
      }

      streamEventCollector.collect(streamEvents.get(size - 1), accountId, new FutureCallback<Void>() {
        @Override
        public void onSuccess(Void result) {
          callback.handleResult(Status.OK);
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.error("Error consuming single event", t);
          callback.handleError(t);
        }
      });
    } catch (Exception e) {
      LOG.warn("Error consuming batch of events", e);
      callback.handleError(e);
    }
  }

  @Override
  public Status append(AvroFlumeEvent event) throws AvroRemoteException {
    CallFuture<Status> callFuture = new CallFuture<Status>();
    try {
      append(event, callFuture);
      return callFuture.get();
    } catch (Exception e) {
      throw new AvroRemoteException(e);
    }
  }

  @Override
  public Status appendBatch(List<AvroFlumeEvent> events) throws AvroRemoteException {
    CallFuture<Status> callFuture = new CallFuture<Status>();
    try {
      appendBatch(events, callFuture);
      return callFuture.get();
    } catch (Exception e) {
      throw new AvroRemoteException(e);
    }
  }

  /**
   * Converts a Flume event to a StreamEvent. This is a pure copy of the headers
   * and body. In addition, the collector name header is set.
   *
   *
   * @param flumeEvent the flume event to be converted
   * @param accountId  id of account used to send events
   * @return the resulting event
   */
  protected StreamEvent convertFlume2Event(AvroFlumeEvent flumeEvent, String accountId) throws Exception {

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
    headers.put(Constants.HEADER_FROM_COLLECTOR, com.continuuity.common.conf.Constants.Gateway.FLUME_HANDLER_NAME);

    if (destination == null) {
      throw new Exception("Cannot enqueue event without destination stream");
    }

    DefaultStreamEvent event = new DefaultStreamEvent(headers, flumeEvent.getBody());
    if (!streamCache.validateStream(accountId, destination)) {
      throw new Exception(String.format("Cannot enqueue event to non-existent stream '%s'.", destination));
    }
    return event;
  }

  /**
   * Converts a batch of Flume event to a lis of Events, using @ref
   * convertFlume2Event.
   *
   *
   * @param flumeEvents the flume events to be converted
   * @param accountId   id of account used to send events
   * @return the resulting events
   */
  protected List<StreamEvent> convertFlume2Events(List<AvroFlumeEvent> flumeEvents, String accountId)
    throws Exception {
    List<StreamEvent> events = new ArrayList<StreamEvent>(flumeEvents.size());
    for (AvroFlumeEvent flumeEvent : flumeEvents) {
      events.add(convertFlume2Event(flumeEvent, accountId));
    }
    return events;
  }

  /**
   * Callback that does not do anything.
   */
  private static final FutureCallback<Void> NO_OP_CALLBACK = new FutureCallback<Void>() {
    @Override
    public void onSuccess(Void result) {
      // no-op
    }

    @Override
    public void onFailure(Throwable t) {
      // no-op
    }
  };
}
