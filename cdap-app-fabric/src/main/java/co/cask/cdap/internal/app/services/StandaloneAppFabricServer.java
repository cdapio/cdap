/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceEnsurer;
import co.cask.cdap.internal.app.runtime.artifact.SystemArtifactLoader;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.http.HttpHandler;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.discovery.DiscoveryService;

import java.net.InetAddress;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * App Fabric server in standalone mode.
 */
public class StandaloneAppFabricServer extends AppFabricServer {

  private final MetricStore metricStore;

  /**
   * Construct the Standalone AppFabricServer with service factory and configuration coming from guice injection.
   */
  @Inject
  public StandaloneAppFabricServer(CConfiguration configuration,
                                   DiscoveryService discoveryService,
                                   SchedulerService schedulerService,
                                   NotificationService notificationService,
                                   @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                                   @Named(Constants.AppFabric.HANDLERS_BINDING) Set<HttpHandler> handlers,
                                   @Nullable MetricsCollectionService metricsCollectionService,
                                   ProgramRuntimeService programRuntimeService,
                                   ApplicationLifecycleService applicationLifecycleService,
                                   ProgramLifecycleService programLifecycleService,
                                   StreamCoordinatorClient streamCoordinatorClient,
                                   @Named("appfabric.services.names") Set<String> servicesNames,
                                   @Named("appfabric.handler.hooks") Set<String> handlerHookNames,
                                   DefaultNamespaceEnsurer defaultNamespaceEnsurer,
                                   MetricStore metricStore,
                                   SystemArtifactLoader systemArtifactLoader) {
    super(configuration, discoveryService, schedulerService, notificationService, hostname, handlers,
          metricsCollectionService, programRuntimeService, applicationLifecycleService,
          programLifecycleService, streamCoordinatorClient, servicesNames, handlerHookNames, defaultNamespaceEnsurer,
          systemArtifactLoader);
    this.metricStore = metricStore;
  }

  @Override
  protected void startUp() throws Exception {
    // before starting up, we need to delete the queue.pending metric for all queues of all flows. This is
    // because queues are in-memory and lost upon Standalone restart. This must happen before app-fabric
    // starts, that is, before any flows can get started.
    FlowUtils.deleteFlowPendingMetrics(metricStore, null, null, null);
    super.startUp();
  }
}
