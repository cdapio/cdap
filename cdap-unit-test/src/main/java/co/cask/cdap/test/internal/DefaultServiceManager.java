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

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.test.AbstractProgramManager;
import co.cask.cdap.test.MetricsManager;
import co.cask.cdap.test.ServiceManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * A default implementation of {@link ServiceManager}.
 */
public class DefaultServiceManager extends AbstractProgramManager<ServiceManager> implements ServiceManager {

  private final DiscoveryServiceClient discoveryServiceClient;
  private final AppFabricClient appFabricClient;

  private final MetricsManager metricsManager;

  public DefaultServiceManager(ProgramId programId,
                               AppFabricClient appFabricClient, DiscoveryServiceClient discoveryServiceClient,
                               DefaultApplicationManager applicationManager, MetricsManager metricsManager) {
    super(programId, applicationManager);

    this.discoveryServiceClient = discoveryServiceClient;
    this.appFabricClient = appFabricClient;
    this.metricsManager = metricsManager;
  }

  @Override
  public void setInstances(int instances) {
    Preconditions.checkArgument(instances > 0, "Instance count should be > 0.");
    try {
      appFabricClient.setServiceInstances(programId.getNamespace(), programId.getApplication(),
                                          programId.getProgram(), instances);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getRequestedInstances() {
    ServiceInstances instances = getInstances();
    return instances.getRequested();
  }

  @Override
  public int getProvisionedInstances() {
    ServiceInstances instances = getInstances();
    return instances.getProvisioned();
  }

  private ServiceInstances getInstances() {
    try {
      return appFabricClient.getServiceInstances(programId.getNamespace(), programId.getApplication(),
                                                 programId.getProgram());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public URL getServiceURL() {
    return getServiceURL(1, TimeUnit.SECONDS);
  }

  @Override
  public URL getServiceURL(long timeout, TimeUnit timeoutUnit) {
    ServiceDiscovered discovered = discoveryServiceClient.discover(ServiceDiscoverable.getName(programId));
    return ServiceDiscoverable.createServiceBaseURL(new RandomEndpointStrategy(discovered).pick(timeout, timeoutUnit),
                                                    programId);
  }

  @Override
  public RuntimeMetrics getMetrics() {
    return metricsManager.getServiceMetrics(programId.getNamespace(), programId.getApplication(),
                                            programId.getProgram());
  }
}
