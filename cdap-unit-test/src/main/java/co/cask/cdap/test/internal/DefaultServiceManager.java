/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.common.discovery.ServiceDiscoveries;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.cdap.test.AbstractServiceManager;
import co.cask.cdap.test.ServiceManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * A default implementation of {@link ServiceManager}.
 */
public class DefaultServiceManager extends AbstractServiceManager {

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
    ServiceInstances instances = getRunnableInstances(runnableName);
    return instances.getRequested();
  }

  @Override
  public int getProvisionedInstances(String runnableName) {
    ServiceInstances instances = getRunnableInstances(runnableName);
    return instances.getProvisioned();
  }

  private ServiceInstances getRunnableInstances(String runnableName) {
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
    String discoveryName = String.format("service.%s.%s.%s", accountId, applicationId, serviceName);
    String path = String.format("%s/namespaces/%s/apps/%s/services/%s/methods/",
                                Constants.Gateway.API_VERSION_3, accountId, applicationId, serviceName);
    return ServiceDiscoveries.discoverURL(discoveryServiceClient, discoveryName, "http", path, timeout, timeoutUnit);
  }
}
