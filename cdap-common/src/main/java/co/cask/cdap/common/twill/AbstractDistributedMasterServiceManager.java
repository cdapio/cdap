/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.common.twill;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.SystemServiceLiveInfo;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class that can be extended by individual CDAP Services to implement their management methods.
 */
public abstract class AbstractDistributedMasterServiceManager implements MasterServiceManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDistributedMasterServiceManager.class);
  private static final long SERVICE_PING_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS);
  protected final long discoveryTimeout;

  protected CConfiguration cConf;
  protected TwillRunnerService twillRunnerService;
  protected String serviceName;
  protected DiscoveryServiceClient discoveryServiceClient;

  public AbstractDistributedMasterServiceManager(CConfiguration cConf, String serviceName,
                                                 TwillRunnerService twillRunnerService,
                                                 DiscoveryServiceClient discoveryServiceClient) {
    this.cConf = cConf;
    this.serviceName = serviceName;
    this.twillRunnerService = twillRunnerService;
    this.discoveryTimeout = cConf.getLong(Constants.Monitor.DISCOVERY_TIMEOUT_SECONDS);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public SystemServiceLiveInfo getLiveInfo() {
    SystemServiceLiveInfo.Builder builder = SystemServiceLiveInfo.builder();

    Iterable<TwillController> twillControllerList = twillRunnerService.lookup(Constants.Service.MASTER_SERVICES);
    if (twillControllerList == null) {
      return builder.build();
    }

    for (TwillController twillController : twillControllerList) {
      if (twillController.getResourceReport() == null) {
        continue;
      }

      ResourceReport resourceReport = twillController.getResourceReport();
      Collection<TwillRunResources> runResources = resourceReport.getResources().get(serviceName);
      for (TwillRunResources resources : runResources) {
        Containers.ContainerInfo containerInfo = new Containers.ContainerInfo(
          Containers.ContainerType.SYSTEM_SERVICE,
          serviceName,
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
    Iterable<TwillController> twillControllerList = twillRunnerService.lookup(Constants.Service.MASTER_SERVICES);
    int instances = 0;
    if (twillControllerList != null) {
      for (TwillController twillController : twillControllerList) {
        if (twillController.getResourceReport() != null) {
          instances = twillController.getResourceReport().getRunnableResources(serviceName).size();
        }
      }
    }
    return instances;
  }

  @Override
  public boolean isServiceEnabled() {
    // By default all the services are enabled. extending classes can override if the behavior should be different.
    return true;
  }

  @Override
  public boolean setInstances(int instanceCount) {
    Preconditions.checkArgument(instanceCount > 0);
    try {
      Iterable<TwillController> twillControllerList = twillRunnerService.lookup(Constants.Service.MASTER_SERVICES);
      if (twillControllerList != null) {
        for (TwillController twillController : twillControllerList) {
          twillController.changeInstances(serviceName, instanceCount).get();
        }
      }
      return true;
    } catch (Throwable t) {
      LOG.error("Could not change service instance of {} : {}", serviceName, t.getMessage(), t);
      return false;
    }
  }

  @Override
  public int getMinInstances() {
    return 1;
  }

  @Override
  public boolean canCheckStatus() {
    return true;
  }

  @Override
  public boolean isLogAvailable() {
    return true;
  }

  @Override
  public boolean isServiceAvailable() {
    String url = null;
    try {
      Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(getDiscoverableName());
      for (Discoverable discoverable : discoverables) {
        String scheme = Arrays.equals(Constants.Security.SSL_URI_SCHEME.getBytes(), discoverable.getPayload()) ?
          Constants.Security.SSL_URI_SCHEME : Constants.Security.URI_SCHEME;
        url = String.format("%s%s:%d/ping", scheme, discoverable.getSocketAddress().getHostName(),
                            discoverable.getSocketAddress().getPort());
        //Ping the discovered service to check its status.
        if (checkGetStatus(url).equals(HttpResponseStatus.OK)) {
          return true;
        }
      }
      return false;
    } catch (IllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Unable to ping {} at {} : Reason : {}", serviceName, url, e.getMessage());
      return false;
    }
  }

  protected String getDiscoverableName() {
    return serviceName;
  }

  protected final HttpResponseStatus checkGetStatus(String url) throws Exception {
    HttpURLConnection httpConn = null;
    try {
      httpConn = (HttpURLConnection) new URL(url).openConnection();
      httpConn.setConnectTimeout((int) SERVICE_PING_RESPONSE_TIMEOUT);
      httpConn.setReadTimeout((int) SERVICE_PING_RESPONSE_TIMEOUT);
      return (HttpResponseStatus.valueOf(httpConn.getResponseCode()));
    } catch (SocketTimeoutException e) {
      return HttpResponseStatus.NOT_FOUND;
    } finally {
      if (httpConn != null) {
        httpConn.disconnect();
      }
    }
  }

  @Override
  public void restartAllInstances() {
    Iterable<TwillController> twillControllers = twillRunnerService.lookup(Constants.Service.MASTER_SERVICES);
    for (TwillController twillController : twillControllers) {
      // Call restart instances
      Futures.getUnchecked(twillController.restartAllInstances(serviceName));
    }
  }

  @Override
  public void restartInstances(int instanceId, int... moreInstanceIds) {
    Iterable<TwillController> twillControllers = twillRunnerService.lookup(Constants.Service.MASTER_SERVICES);
    for (TwillController twillController : twillControllers) {
      // Call restart instances
      Futures.getUnchecked(twillController.restartInstances(serviceName, instanceId, moreInstanceIds));
    }
  }

  @Override
  public void updateServiceLogLevels(Map<String, LogEntry.Level> logLevels) {
    Iterable<TwillController> twillControllers = twillRunnerService.lookup(Constants.Service.MASTER_SERVICES);
    for (TwillController twillController : twillControllers) {
      // Call update log levels
      Futures.getUnchecked(twillController.updateLogLevels(serviceName, logLevels));
    }
  }

  @Override
  public void resetServiceLogLevels(Set<String> loggerNames) {
    Iterable<TwillController> twillControllers = twillRunnerService.lookup(Constants.Service.MASTER_SERVICES);
    for (TwillController twillController : twillControllers) {
      // Call reset log levels
      Futures.getUnchecked(twillController.resetRunnableLogLevels(serviceName,
                                                                  loggerNames.toArray(new String[loggerNames.size()])));
    }
  }
}
