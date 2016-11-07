/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.preview;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.http.HandlerHook;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;

/**
 * Preview Server which is responsible for managing http service that handles the preview request.
 */
public class PreviewServer extends AbstractIdleService {

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancellable;

  /**
   * Creates Preview Injector based on shared bindings passed from Standalone injector and
   * constructs the PreviewServer with configuration coming from guice injection.
   */
  @Inject
  PreviewServer(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                DiscoveryService discoveryService,
                @Named(Constants.Preview.HANDLERS_BINDING) Set<HttpHandler> handlers) {

    String address = cConf.get(Constants.Preview.ADDRESS);

    // Create handler hooks
    this.httpService = new CommonNettyHttpServiceBuilder(cConf)
      .setHost(address)
      .setHandlerHooks(Collections.singleton(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.PREVIEW_HTTP)))
      .addHttpHandlers(handlers)
      .setConnectionBacklog(cConf.getInt(Constants.AppFabric.BACKLOG_CONNECTIONS,
                                         Constants.AppFabric.DEFAULT_BACKLOG))
      .setExecThreadPoolSize(cConf.getInt(Constants.AppFabric.EXEC_THREADS,
                                          Constants.AppFabric.DEFAULT_EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.AppFabric.BOSS_THREADS,
                                          Constants.AppFabric.DEFAULT_BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.AppFabric.WORKER_THREADS,
                                            Constants.AppFabric.DEFAULT_WORKER_THREADS))
      .build();
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {

    // Run http service on random port
    httpService.startAndWait();

    // cancel service discovery on shutdown
    cancellable = discoveryService.register(ResolvingDiscoverable.of(new Discoverable(Constants.Service.PREVIEW_HTTP,
                                                                                      httpService.getBindAddress())));
  }

  @Override
  protected void shutDown() throws Exception {
    cancellable.cancel();
    httpService.stopAndWait();
  }
}
