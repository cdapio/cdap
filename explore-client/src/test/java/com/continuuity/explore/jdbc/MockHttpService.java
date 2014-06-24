package com.continuuity.explore.jdbc;

import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * A generic http service for testing router.
 */
public class MockHttpService extends AbstractIdleService {
  private static final Logger log = LoggerFactory.getLogger(MockHttpService.class);

  private final Set<HttpHandler> httpHandlers;

  private NettyHttpService httpService;
  private int port;

  public MockHttpService(HttpHandler... httpHandlers) {
    this.httpHandlers = ImmutableSet.copyOf(httpHandlers);
  }

  @Override
  protected void startUp() throws Exception {
    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(httpHandlers);
    builder.setHost("localhost");
    builder.setPort(0);
    httpService = builder.build();
    httpService.startAndWait();

    port = httpService.getBindAddress().getPort();
    log.info("Started test server on {}", httpService.getBindAddress());
  }

  public int getPort() {
    return port;
  }

  @Override
  protected void shutDown() throws Exception {
    httpService.stopAndWait();
  }
}
