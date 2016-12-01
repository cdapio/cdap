/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.test.AbstractProgramManager;
import co.cask.cdap.test.ServiceManager;
import com.google.common.base.Throwables;

import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Remote implementation of {@link ServiceManager}.
 */
public class RemoteServiceManager extends AbstractProgramManager<ServiceManager> implements ServiceManager {

  private final MetricsClient metricsClient;
  private final ProgramClient programClient;
  private final ServiceClient serviceClient;
  private final ServiceId serviceId;

  public RemoteServiceManager(ServiceId serviceId, ClientConfig clientConfig, RESTClient restClient,
                              RemoteApplicationManager remoteApplicationManager) {
    super(serviceId, remoteApplicationManager);
    this.serviceId = serviceId;
    this.metricsClient = new MetricsClient(clientConfig, restClient);
    this.programClient = new ProgramClient(clientConfig, restClient);
    this.serviceClient = new ServiceClient(clientConfig, restClient);
  }

  @Override
  public void setInstances(int instances) {
    try {
      programClient.setServiceInstances(serviceId.toId(), instances);
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
      return programClient.getServiceInstances(serviceId.toId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public URL getServiceURL() {
    return getServiceURL(30, TimeUnit.SECONDS);
  }

  @Override
  public URL getServiceURL(long timeout, TimeUnit timeoutUnit) {
    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          try {
            serviceClient.checkAvailability(serviceId);
            return true;
          } catch (ServiceUnavailableException e) {
            // simply retry in case its not yet available
            return false;
          }
        }
      }, timeout, timeoutUnit);
      return serviceClient.getVersionedServiceURL(serviceId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public RuntimeMetrics getMetrics() {
    return metricsClient.getServiceMetrics(programId.toId());
  }
}
