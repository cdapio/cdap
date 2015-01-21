/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.hooks.MetricsReporterHook;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.jboss.netty.buffer.HeapChannelBufferFactory;

import java.net.InetSocketAddress;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A Http service endpoint that host the stream handler.
 */
public final class StreamHttpService extends AbstractIdleService implements Supplier<Discoverable> {

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancellable;
  private Discoverable discoverable;

  @Inject
  public StreamHttpService(CConfiguration cConf, DiscoveryService discoveryService,
                           @Named(Constants.Stream.STREAM_HANDLER) Set<HttpHandler> handlers,
                           @Nullable MetricsCollectionService metricsCollectionService) {
    this.discoveryService = discoveryService;

    int workerThreads = cConf.getInt(Constants.Stream.WORKER_THREADS, 10);
    this.httpService = new CommonNettyHttpServiceBuilder(cConf)
      .addHttpHandlers(handlers)
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.Stream.STREAM_HANDLER)))
      .setHost(cConf.get(Constants.Stream.ADDRESS))
      .setWorkerThreadPoolSize(workerThreads)
      .setExecThreadPoolSize(0)         // Execution happens in the io worker thread directly
      .setConnectionBacklog(20000)
      .setChannelConfig("child.bufferFactory",
                        HeapChannelBufferFactory.getInstance()) // ChannelBufferFactory that always creates new Buffer
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.STREAMS));
    httpService.startAndWait();

    discoverable = new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.STREAMS;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    };
    cancellable = discoveryService.register(discoverable);
}

  @Override
  protected void shutDown() throws Exception {
    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      httpService.stopAndWait();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }

  /**
   * Get the address the server has bound to.
   *
   * @return socket address the server has bound to.
   */
  public InetSocketAddress getBindAddress() {
    return discoverable.getSocketAddress();
  }

  @Override
  public Discoverable get() {
    return discoverable;
  }
}
