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

package io.cdap.cdap.test.remote;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.metrics.RuntimeMetrics;
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.ServiceClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.test.AbstractProgramManager;
import io.cdap.cdap.test.ServiceManager;

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
      programClient.setServiceInstances(serviceId, instances);
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
      return programClient.getServiceInstances(serviceId);
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
    return metricsClient.getServiceMetrics(serviceId);
  }
}
