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

package io.cdap.cdap.common.twill;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.proto.Containers;
import io.cdap.cdap.proto.SystemServiceLiveInfo;
import io.cdap.cdap.security.URIScheme;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

/**
 * An abstract base class to provide common implementation for the {@link MasterServiceManager}.
 */
public abstract class AbstractMasterServiceManager implements MasterServiceManager {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMasterServiceManager.class);
  private static final Logger OUTAGE_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(30000));

  private final CConfiguration cConf;
  private final String serviceName;
  private final DiscoveryServiceClient discoveryClient;
  private final HttpRequestConfig httpRequestConfig;
  private final TwillRunner twillRunner;
  private final int serviceTimeoutSeconds;

  protected AbstractMasterServiceManager(CConfiguration cConf, DiscoveryServiceClient discoveryClient,
                                         String serviceName, TwillRunner twillRunner) {
    this.cConf = cConf;
    this.discoveryClient = discoveryClient;
    this.serviceName = serviceName;
    this.serviceTimeoutSeconds = cConf.getInt(Constants.Monitor.DISCOVERY_TIMEOUT_SECONDS);

    int requestTimeout = serviceTimeoutSeconds * 1000;
    this.httpRequestConfig = new HttpRequestConfig(requestTimeout, requestTimeout, false);
    this.twillRunner = twillRunner;
  }

  /**
   * Returns the {@link CConfiguration}.
   */
  protected final CConfiguration getCConf() {
    return cConf;
  }

  /**
   * Returns the name of the twill runnable.
   */
  protected String getTwillRunnableName() {
    return serviceName;
  }

  @Override
  public boolean isServiceAvailable() {
    // Try to ping the endpoint. If any one of them is available, we treat the service as available.
    ServiceDiscovered serviceDiscovered = discoveryClient.discover(serviceName);
    // Block to wait for some endpoint to be available. This is just to compensate initialization.
    // Calls after the first discovery will return immediately.
    new RandomEndpointStrategy(() -> serviceDiscovered).pick(serviceTimeoutSeconds, TimeUnit.SECONDS);
    return StreamSupport.stream(serviceDiscovered.spliterator(), false).anyMatch(this::isEndpointAlive);
  }

  @Override
  public SystemServiceLiveInfo getLiveInfo() {
    SystemServiceLiveInfo.Builder builder = SystemServiceLiveInfo.builder();

    for (TwillController twillController : twillRunner.lookup(Constants.Service.MASTER_SERVICES)) {
      if (twillController.getResourceReport() == null) {
        continue;
      }

      ResourceReport resourceReport = twillController.getResourceReport();
      Collection<TwillRunResources> runResources = resourceReport.getResources().getOrDefault(getTwillRunnableName(),
                                                                                              Collections.emptyList());
      for (TwillRunResources resources : runResources) {
        Containers.ContainerInfo containerInfo = new Containers.ContainerInfo(
          Containers.ContainerType.SYSTEM_SERVICE,
          getTwillRunnableName(),
          resources.getInstanceId(),
          resources.getContainerId(),
          resources.getHost(),
          resources.getMemoryMB(),
          resources.getVirtualCores(),
          resources.getDebugPort());
        builder.addContainer(containerInfo);
      }
    }
    return builder.build();
  }

  @Override
  public int getInstances() {
    for (TwillController twillController : twillRunner.lookup(Constants.Service.MASTER_SERVICES)) {
      ResourceReport resourceReport = twillController.getResourceReport();
      if (resourceReport != null) {
        int instances = resourceReport.getRunnableResources(getTwillRunnableName()).size();
        if (instances > 0) {
          return instances;
        }
      }
    }
    return 0;
  }

  @Override
  public boolean setInstances(int instanceCount) {
    Preconditions.checkArgument(instanceCount > 0);
    try {
      for (TwillController twillController : twillRunner.lookup(Constants.Service.MASTER_SERVICES)) {
        twillController.changeInstances(getTwillRunnableName(), instanceCount).get();
      }
      return true;
    } catch (Throwable t) {
      LOG.error("Could not change service instance of {} : {}", getTwillRunnableName(), t.getMessage(), t);
      return false;
    }
  }

  @Override
  public void restartAllInstances() {
    for (TwillController twillController : twillRunner.lookup(Constants.Service.MASTER_SERVICES)) {
      // Call restart instances
      Futures.getUnchecked(twillController.restartAllInstances(getTwillRunnableName()));
    }
  }

  @Override
  public void restartInstances(int instanceId, int... moreInstanceIds) {
    for (TwillController twillController : twillRunner.lookup(Constants.Service.MASTER_SERVICES)) {
      // Call restart instances
      Futures.getUnchecked(twillController.restartInstances(getTwillRunnableName(), instanceId, moreInstanceIds));
    }
  }

  @Override
  public void updateServiceLogLevels(Map<String, LogEntry.Level> logLevels) {
    for (TwillController twillController : twillRunner.lookup(Constants.Service.MASTER_SERVICES)) {
      // Call update log levels
      Futures.getUnchecked(twillController.updateLogLevels(getTwillRunnableName(), logLevels));
    }
  }

  @Override
  public void resetServiceLogLevels(Set<String> loggerNames) {
    for (TwillController twillController : twillRunner.lookup(Constants.Service.MASTER_SERVICES)) {
      // Call reset log levels
      Futures.getUnchecked(twillController.resetRunnableLogLevels(getTwillRunnableName(),
                                                                  loggerNames.toArray(new String[0])));
    }
  }

  private boolean isEndpointAlive(Discoverable discoverable) {
    try {
      URL url = URIScheme.createURI(discoverable, "/ping").toURL();
      int responseCode = HttpRequests.execute(HttpRequest.get(url).build(), httpRequestConfig).getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        return true;
      }
    } catch (IOException e) {
      OUTAGE_LOG.warn("Failed to ping endpoint from discoverable {} for service {}", discoverable, serviceName, e);
    }
    return false;
  }
}
