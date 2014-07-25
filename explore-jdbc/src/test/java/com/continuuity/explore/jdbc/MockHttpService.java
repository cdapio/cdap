/*
 * Copyright 2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
