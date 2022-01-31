/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.Service;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.AbstractModule;
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.MonitorHandlerModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.store.ServiceStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.service.HealthCheckService;
import io.cdap.cdap.common.service.RetryOnStartFailureService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceLauncher;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.operations.OperationalStatsService;
import io.cdap.cdap.operations.guice.OperationalStatsModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.store.SecureStoreService;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Config;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * The main class to run app-fabric and other supporting services.
 */
public class AppFabricServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServiceMain.class);

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(AppFabricServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
                                           EnvironmentOptions options, CConfiguration cConf) {
    return Arrays.asList(
      // Always use local table implementations, which use LevelDB.
      // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
      new DataSetServiceModules().getStandaloneModules(),
      // The Dataset set modules are only needed to satisfy dependency injection
      new DataSetsModules().getStandaloneModules(),
      new MetricsStoreModule(),
      new MessagingClientModule(),
      new ExploreClientModule(),
      new AuditModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      Modules.override(new AppFabricServiceRuntimeModule(cConf).getDistributedModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
        }
      }),
      new ProgramRunnerRuntimeModule().getDistributedModules(true),
      new MonitorHandlerModule(false),
      new SecureStoreServerModule(),
      new OperationalStatsModule(),
      getDataFabricModule(),
      new DFSLocationModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(TwillRunnerService.class).toProvider(
            new SupplierProviderBridge<>(masterEnv.getTwillRunnerSupplier())).in(Scopes.SINGLETON);
          bind(TwillRunner.class).to(TwillRunnerService.class);

          // TODO (CDAP-14677): find a better way to inject metadata publisher
          bind(MetadataPublisher.class).to(MessagingMetadataPublisher.class);
          bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
        }
      }
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    if (SecurityUtil.isInternalAuthEnabled(cConf)) {
      services.add(injector.getInstance(TokenManager.class));
    }
    closeableResources.add(injector.getInstance(AccessControllerInstantiator.class));
    services.add(injector.getInstance(OperationalStatsService.class));
    services.add(injector.getInstance(SecureStoreService.class));
    services.add(injector.getInstance(DatasetOpExecutorService.class));
    services.add(injector.getInstance(ServiceStore.class));

    try {
      String host = cConf.get(Constants.AppFabricHealthCheck.SERVICE_BIND_ADDRESS);
      int port = cConf.getInt(Constants.AppFabricHealthCheck.SERVICE_BIND_PORT);
      HealthCheckService healthCheckService = injector.getInstance(HealthCheckService.class);
      healthCheckService.initiate(host, port, Constants.AppFabricHealthCheck.APP_FABRIC_HEALTH_CHECK_SERVICE);
      services.add(healthCheckService);

      ApiClient client = Config.defaultClient();
      CoreV1Api coreApi = new CoreV1Api(client);
      KubeMasterEnvironment kubeMasterEnv = (KubeMasterEnvironment) MasterEnvironments.getMasterEnvironment();
      JsonArray podArray = getPodInfoArray(coreApi, kubeMasterEnv);
      JsonArray nodeArray = getNodeInfoArray(coreApi);
//      JsonArray eventArray = getEventInfoArray(coreApi, kubeMasterEnv);
      cConf.set(Constants.AppFabricHealthCheck.POD_INFO, podArray.toString());
      cConf.set(Constants.AppFabricHealthCheck.NODE_INFO, nodeArray.toString());
      cConf.set(Constants.AppFabricHealthCheck.EVENT_INFO, "eventArray.toString()");
      cConf.set(Constants.AppFabricHealthCheck.SERVICE_NAME_WITH_POD, Constants.AppFabricHealthCheck.POD_INFO);
      cConf.set(Constants.AppFabricHealthCheck.SERVICE_NAME_WITH_NODE, Constants.AppFabricHealthCheck.NODE_INFO);
      cConf.set(Constants.AppFabricHealthCheck.SERVICE_NAME_WITH_EVENT, Constants.AppFabricHealthCheck.EVENT_INFO);
    } catch (IOException e) {
      LOG.error("Can not get api client ", e);
    }
    Binding<ZKClientService> zkBinding = injector.getExistingBinding(Key.get(ZKClientService.class));
    if (zkBinding != null) {
      services.add(zkBinding.getProvider().get());
    }


    // Start both the remote TwillRunnerService and regular TwillRunnerService
    TwillRunnerService remoteTwillRunner = injector.getInstance(Key.get(TwillRunnerService.class,
                                                                        Constants.AppFabric.RemoteExecution.class));
    services.add(new TwillRunnerServiceWrapper(remoteTwillRunner));
    services.add(new TwillRunnerServiceWrapper(injector.getInstance(TwillRunnerService.class)));
    services.add(new RetryOnStartFailureService(() -> injector.getInstance(DatasetService.class),
                                                RetryStrategies.exponentialDelay(200, 5000, TimeUnit.MILLISECONDS)));
    services.add(injector.getInstance(AppFabricServer.class));

    if (cConf.getBoolean(Constants.TaskWorker.POOL_ENABLE)) {
      services.add(injector.getInstance(TaskWorkerServiceLauncher.class));
    }

    // Optionally adds the master environment task
    masterEnv.getTask().ifPresent(task -> services.add(new MasterTaskExecutorService(task, masterEnvContext)));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.APP_FABRIC_HTTP);
  }

  private JsonArray getPodInfoArray(CoreV1Api coreApi, KubeMasterEnvironment kubeMasterEnv) {
    JsonArray podArray = new JsonArray();
    try {
      if (kubeMasterEnv != null) {
        PodInfo podInfo = kubeMasterEnv.getPodInfo();
        String labelSelector = podInfo.getLabels()
          .entrySet()
          .stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining(","));
        V1PodList podList =
          coreApi.listNamespacedPod(podInfo.getNamespace(), null, null, null, null, labelSelector, null, null, null,
                                    null, null);
        Set<String> activePods = podList.getItems()
          .stream()
          .map(V1Pod::getMetadata)
          .filter(Objects::nonNull)
          .map(V1ObjectMeta::getName)
          .collect(Collectors.toSet());
        for (V1Pod pod : podList.getItems()) {
          if (pod.getMetadata() == null || !activePods.contains(pod.getMetadata().getName())) {
            continue;
          }
          String podName = pod.getMetadata().getName();
          JsonObject podObject = new JsonObject();
          podObject.addProperty("name", podName);
          podObject.addProperty("status", pod.getStatus().getMessage());
          String podLog =
            coreApi.readNamespacedPodLog(pod.getMetadata().getName(), pod.getMetadata().getNamespace(), null, null,
                                         null, null, null, null, null, null, null);

          podObject.addProperty("podLog", podLog);
          podArray.add(podObject);
        }
      }
    } catch (ApiException e) {
      LOG.error("Can not obtain api client ", e);
    }
    return podArray;
  }

  private JsonArray getNodeInfoArray(CoreV1Api coreApi) {
    JsonArray nodeArray = new JsonArray();
    try {
      V1NodeList nodeList = coreApi.listNode(null, null, null, null, null, null, null, null, null, null);
      Set<String> activeNodes = nodeList.getItems()
        .stream()
        .map(V1Node::getMetadata)
        .filter(Objects::nonNull)
        .map(V1ObjectMeta::getName)
        .collect(Collectors.toSet());
      for (V1Node node : nodeList.getItems()) {
        if (node.getMetadata() == null || !activeNodes.contains(node.getMetadata().getName())) {
          continue;
        }
        String nodeName = node.getMetadata().getName();
        JsonObject nodeObject = new JsonObject();
        nodeObject.addProperty("name", nodeName);
        nodeObject.addProperty("status", node.getStatus().getConditions().get(0).getStatus());
        nodeArray.add(nodeObject);
      }
    } catch (ApiException e) {
      LOG.error("Can not obtain api client ", e);
    }
    return nodeArray;
  }

//  private JsonArray getEventInfoArray(CoreV1Api coreApi, KubeMasterEnvironment kubeMasterEnv) {
//    JsonArray eventArray = new JsonArray();
//    try {
//      if (kubeMasterEnv != null) {
//        PodInfo podInfo = kubeMasterEnv.getPodInfo();
//        String nameSpace = podInfo.getNamespace();
//        V1EventList eventList = coreApi.listNamespacedEvent(nameSpace, null, null, null, null, null, null);
//        Set<String> activeEvents = eventList.getItems()
//          .stream()
//          .map(V1Event::getMetadata)
//          .filter(Objects::nonNull)
//          .map(V1ObjectMeta::getName)
//          .collect(Collectors.toSet());
//        for (V1Event event : eventList.getItems()) {
//          if (event.getMetadata() == null || !activeEvents.contains(event.getMetadata().getName())) {
//            continue;
//          }
//          String eventName = event.getMetadata().getName();
//          JsonObject eventObject = new JsonObject();
//          eventObject.addProperty("name", eventName);
//          eventObject.addProperty("status", event.getMessage());
//          eventArray.add(eventObject);
//        }
//      }
//    } catch (ApiException e) {
//      LOG.error("Can not obtain api client ", e);
//    }
//    return eventArray;
//  }
}
