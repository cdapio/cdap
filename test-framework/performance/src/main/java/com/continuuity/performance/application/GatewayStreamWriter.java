/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.stream.DefaultStreamEvent;
import com.continuuity.common.stream.StreamEventCodec;
import com.continuuity.performance.gateway.SimpleHttpClient;
import com.continuuity.test.StreamWriter;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Stream writer that sends events to a stream through Gateway's REST API.
 */
public class GatewayStreamWriter implements StreamWriter {

  private final StreamEventCodec codec;
  private final SimpleHttpClient httpClient;

  @Inject
  public GatewayStreamWriter(CConfiguration config,
                             @Assisted QueueName queueName) {

    codec = new StreamEventCodec();
    Map<String, String> headers = null;
    String apiKey = config.get("apikey");
    if (!StringUtils.isEmpty(apiKey)) {
      headers = ImmutableMap.of(Constants.Gateway.CONTINUUITY_API_KEY, apiKey);
    }
    String gateway = config.get(Constants.AppFabric.SERVER_ADDRESS,
                                Constants.AppFabric.DEFAULT_SERVER_ADDRESS);
    if (StringUtils.isEmpty(gateway)) {
      gateway = "localhost";
    }
    // todo
    String url = "Perf framework should be fixed towards new gateway";
    //  Util.findBaseUrl(config, RestCollector.class, null, gateway, -1, apiKey != null)
    //  + queueName.getSimpleName();
    httpClient = new SimpleHttpClient(url, headers);
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
    StreamEvent event = new DefaultStreamEvent(ImmutableMap.copyOf(headers), buffer);
    try {
      httpClient.post(codec.encodePayload(event));
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }
}
