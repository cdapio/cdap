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

package co.cask.cdap.test.remote;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.AbstractProgramManager;
import co.cask.cdap.test.ServiceManager;
import com.google.common.base.Throwables;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Remote implementation of {@link ServiceManager}.
 */
public class RemoteServiceManager extends AbstractProgramManager implements ServiceManager {
  private final MetricsClient metricsClient;
  private final ProgramClient programClient;
  private final ServiceClient serviceClient;

  public RemoteServiceManager(Id.Program programId, ClientConfig clientConfig,
                              RemoteApplicationManager remoteApplicationManager) {
    super(programId, remoteApplicationManager);
    ClientConfig namespacedClientConfig = new ClientConfig.Builder(clientConfig).build();
    namespacedClientConfig.setNamespace(programId.getNamespace());
    this.metricsClient = new MetricsClient(namespacedClientConfig);
    this.programClient = new ProgramClient(namespacedClientConfig);
    this.serviceClient = new ServiceClient(namespacedClientConfig);
  }

  @Override
  public void setInstances(int instances) {
    try {
      programClient.setServiceInstances(programId.getApplicationId(), programId.getId(), instances);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getRequestedInstances() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getProvisionedInstances() {
    try {
      return programClient.getServiceInstances(programId.getApplicationId(), programId.getId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public URL getServiceURL() {
    try {
      return serviceClient.getServiceURL(programId.getApplicationId(), programId.getId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public URL getServiceURL(long timeout, TimeUnit timeoutUnit) {
    // TODO: handle timeout
    return getServiceURL();
  }

  @Override
  public RuntimeMetrics getMetrics() {
    return metricsClient.getServiceMetrics(programId);
  }
}
