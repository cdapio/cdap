/**
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.CallFuture;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * /**
 * This class is the intermediary between the Flume Avro receiver (Flume
 * events come in through Avro RPC) and the consumer that persists the
 * events into a stream. In particular, it is responsible for
 * <ul>
 * <li>Mapping a flume event to a stream event, including the filtering and
 * mapping and adding of headers</li>
 * <li>Depending on the success of the consumer, create an Avro response
 * for the client (the Avro sink in a customer's Flume flow) to indicate
 * success or failure of the ingestion of the event.
 * </li>
 * </ul>
 *
 * The adapter works by simply reading in stream event and submit it to the stream http endpoint
 */
class FlumeAdapter extends AbstractIdleService implements AvroSourceProtocol.Callback {

  private static final Logger LOG = LoggerFactory.getLogger(FlumeAdapter.class);

  private final DiscoveryServiceClient discoveryClient;
  private EndpointStrategy endpointStrategy;

  @Inject
  FlumeAdapter(DiscoveryServiceClient discoveryClient) {
    this.discoveryClient = discoveryClient;
  }

  @Override
  protected void startUp() throws Exception {
    endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(
        Constants.Service.STREAMS)), 1L, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  @Override
  public void append(AvroFlumeEvent event, org.apache.avro.ipc.Callback<Status> callback) throws IOException {
    try {
      // Discover the stream endpoint
      Discoverable endpoint = endpointStrategy.pick();
      if (endpoint == null) {
        callback.handleError(new IllegalStateException("No stream endpoint available. Unable to write to stream."));
        return;
      }

      // Figure out the stream name
      Map<String, String> headers = Maps.newTreeMap();
      String streamName = createHeaders(event, headers);

      // Forward the request
      URL url = new URL(String.format("http://%s:%d/v2/streams/%s",
                                      endpoint.getSocketAddress().getHostName(),
                                      endpoint.getSocketAddress().getPort(),
                                      streamName));
      HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
      try {
        urlConn.setDoOutput(true);

        // Set headers
        for (Map.Entry<String, String> entry : headers.entrySet()) {
          String key = entry.getKey();
          if (!Constants.Gateway.CONTINUUITY_API_KEY.equals(key)
            && !Constants.Gateway.HEADER_DESTINATION_STREAM.equals(key)) {
            key = streamName + "." + key;
          }
          urlConn.setRequestProperty(key, entry.getValue());
        }

        // Write body
        WritableByteChannel output = Channels.newChannel(urlConn.getOutputStream());
        try {
          ByteBuffer body = event.getBody().duplicate();
          while (body.hasRemaining()) {
            output.write(body);
          }
        } finally {
          output.close();
        }

        // Verify response
        int responseCode = urlConn.getResponseCode();
        Preconditions.checkState(responseCode == HttpURLConnection.HTTP_OK, "Status != 200 OK (%s)", responseCode);

        callback.handleResult(Status.OK);
      } finally {
        urlConn.disconnect();
      }

    } catch (Exception e) {
      LOG.error("Error consuming single event", e);
      callback.handleError(e);
    }
  }

  @Override
  public void appendBatch(List<AvroFlumeEvent> events,
                          org.apache.avro.ipc.Callback<Status> callback) throws IOException {

    // Simply go through one by one
    final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
    org.apache.avro.ipc.Callback<Status> batchCallback = new org.apache.avro.ipc.Callback<Status>() {

      @Override
      public void handleResult(Status result) {
        // No-op
      }

      @Override
      public void handleError(Throwable error) {
        failure.compareAndSet(null, error);
      }
    };

    for (AvroFlumeEvent event : events) {
      append(event, batchCallback);
    }

    Throwable throwable = failure.get();
    if (throwable == null) {
      callback.handleResult(Status.OK);
    } else {
      callback.handleError(throwable);
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
   * Creates request header from the flume event and also retrieve the stream name from the event.
   *
   * @param event The flume event
   * @param headers map for filling in the headers
   * @return The name of the stream
   */
  private String createHeaders(AvroFlumeEvent event, Map<String, String> headers) {
    String streamName = null;
    for (Map.Entry<CharSequence, CharSequence> entry : event.getHeaders().entrySet()) {
      String key = entry.getKey().toString();
      String value = entry.getValue().toString();

      headers.put(key, value);
      if (Constants.Gateway.HEADER_DESTINATION_STREAM.equals(key)) {
        streamName = value;
      }
    }

    if (streamName == null) {
      throw new IllegalArgumentException("Missing header '" + Constants.Gateway.HEADER_DESTINATION_STREAM + "'");
    }
    return streamName;
  }
}
