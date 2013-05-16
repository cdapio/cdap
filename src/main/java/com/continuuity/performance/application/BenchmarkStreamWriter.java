package com.continuuity.performance.application;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.queue.QueueName;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.util.Util;
import com.continuuity.passport.PassportConstants;
import com.continuuity.performance.gateway.HttpPoster;
import com.continuuity.performance.gateway.SimpleHttpPoster;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.continuuity.test.StreamWriter;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.commons.lang.StringUtils;
import org.hsqldb.lib.StringUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public class BenchmarkStreamWriter implements StreamWriter {

  private final OperationContext opCtx;
  private final QueueName queueName;
  private final StreamEventCodec codec;
  HttpPoster poster;

  @Inject
  public BenchmarkStreamWriter(CConfiguration config,
                               @Assisted QueueName queueName,
                               @Assisted("accountId") String accountId,
                               @Assisted("applicationId") String applicationId) {

    opCtx = new OperationContext(accountId, applicationId);
    this.queueName = queueName;
    codec = new StreamEventCodec();
    Map<String, String> headers = null;
    String apiKey = config.get("apikey");
    if (!StringUtil.isEmpty(apiKey)) {
      headers = ImmutableMap.of(PassportConstants.CONTINUUITY_API_KEY_HEADER, apiKey);
    }
    String gateway = config.get(Constants.CFG_APP_FABRIC_SERVER_ADDRESS, Constants.DEFAULT_APP_FABRIC_SERVER_ADDRESS);
    if (StringUtils.isEmpty(gateway)) {
      gateway = "localhost";
    }
    String url =  Util.findBaseUrl(config, RestCollector.class, null, gateway, -1, apiKey != null)
      + queueName.getSimpleName();
    poster = new SimpleHttpPoster(url, headers);
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
      poster.post(codec.encodePayload(event));
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }
}
