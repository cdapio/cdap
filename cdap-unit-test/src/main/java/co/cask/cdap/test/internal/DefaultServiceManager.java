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

package co.cask.cdap.test.internal;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.test.ServiceManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A default implementation of {@link ServiceManager}.
 */
public class DefaultServiceManager implements ServiceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultServiceManager.class);

  private final DefaultApplicationManager.ProgramId serviceId;
  private final String accountId;
  private final String applicationId;
  private final String serviceName;

  private final DiscoveryServiceClient discoveryServiceClient;
  private final AppFabricClient appFabricClient;
  private final DefaultApplicationManager applicationManager;

  public DefaultServiceManager(String accountId, DefaultApplicationManager.ProgramId serviceId,
                               AppFabricClient appFabricClient, DiscoveryServiceClient discoveryServiceClient,
                               DefaultApplicationManager applicationManager) {
    this.serviceId = serviceId;
    this.accountId = accountId;
    this.applicationId = serviceId.getApplicationId();
    this.serviceName = serviceId.getRunnableId();

    this.discoveryServiceClient = discoveryServiceClient;
    this.appFabricClient = appFabricClient;
    this.applicationManager = applicationManager;

  }

  @Override
  public void setRunnableInstances(String runnableName, int instances) {
    Preconditions.checkArgument(instances > 0, "Instance counter should be > 0.");
    try {
      appFabricClient.setRunnableInstances(applicationId, serviceName, runnableName, instances);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getRequestedInstances(String runnableName) {
    Map<String, String> instances = getRunnableInstances(runnableName);
    return Integer.parseInt(instances.get("requested"));
  }

  @Override
  public int getProvisionedInstances(String runnableName) {
    Map<String, String> instances = getRunnableInstances(runnableName);
    return Integer.parseInt(instances.get("provisioned"));
  }

  private Map<String, String> getRunnableInstances(String runnableName) {
    try {
      return appFabricClient.getRunnableInstances(applicationId, serviceName, runnableName);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    applicationManager.stopProgram(serviceId);
  }

  public boolean isRunning() {
    return applicationManager.isRunning(serviceId);
  }

  @Override
  public URL getServiceURL() {
    return getServiceURL(1, TimeUnit.SECONDS);
  }

  @Override
  public URL getServiceURL(long timeout, TimeUnit timeoutUnit) {
    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover(String.format("service.%s.%s.%s",
                                                                                        accountId,
                                                                                        applicationId,
                                                                                        serviceName));
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(serviceDiscovered);
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable != null) {
      return createURL(discoverable, applicationId, serviceName);
    }

    final SynchronousQueue<URL> discoverableQueue = new SynchronousQueue<URL>();
    Cancellable discoveryCancel = serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        try {
          URL url = createURL(serviceDiscovered.iterator().next(), applicationId, serviceName);
          discoverableQueue.offer(url);
        } catch (NoSuchElementException e) {
          LOG.debug("serviceDiscovered is empty");
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    try {
      URL url = discoverableQueue.poll(timeout, timeoutUnit);
      if (url == null) {
        LOG.debug("Discoverable endpoint not found for appID: {}, serviceName: {}.", applicationId, serviceName);
      }
      return url;
    } catch (InterruptedException e) {
      LOG.error("Got exception: ", e);
      return null;
    } finally {
      discoveryCancel.cancel();
    }
  }

  private URL createURL(@Nullable Discoverable discoverable, String applicationId, String serviceName) {
    if (discoverable == null) {
      return null;
    }
    String hostName = discoverable.getSocketAddress().getHostName();
    int port = discoverable.getSocketAddress().getPort();
    String path = String.format("http://%s:%d%s/namespaces/%s/apps/%s/services/%s/methods/", hostName, port,
                                Constants.Gateway.API_VERSION_3, accountId, applicationId, serviceName);
    try {
      return new URL(path);
    } catch (MalformedURLException e) {
      LOG.error("Got exception while creating serviceURL", e);
      return null;
    }
  }
}
