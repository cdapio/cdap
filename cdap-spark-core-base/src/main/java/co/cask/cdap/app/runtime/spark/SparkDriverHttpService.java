/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.util.concurrent.AbstractIdleService;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * An HTTP service that runs in the Spark driver process for providing various functionalities to executors,
 * such as transaction and file services.
 */
final class SparkDriverHttpService extends AbstractIdleService {

  private final NettyHttpService httpServer;

  SparkDriverHttpService(String programName, String hostname, HttpHandler...handlers) {
    this.httpServer = NettyHttpService.builder(programName + "-http-service")
      .setHttpHandlers(handlers)
      .setExceptionHandler(new HttpExceptionHandler())
      .setHost(hostname)
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    httpServer.start();
  }

  @Override
  protected void shutDown() throws Exception {
    httpServer.stop();
  }

  /**
   * Returns the base {@link URI} for talking to this service remotely through HTTP.
   */
  URI getBaseURI() {
    InetSocketAddress bindAddress = httpServer.getBindAddress();
    if (bindAddress == null) {
      throw new IllegalStateException("SparkDriverHttpService hasn't been started");
    }
    return URI.create(String.format("http://%s:%d", bindAddress.getHostName(), bindAddress.getPort()));
  }
}
