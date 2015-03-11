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
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.AbstractServiceManager;
import com.google.common.base.Throwables;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RemoteServiceManager extends AbstractServiceManager {

  private final Id.Service serviceId;
  private final MetricsClient metricsClient;
  private final ClientConfig clientConfig;

  public RemoteServiceManager(Id.Service serviceId, ClientConfig clientConfig) {
    this.serviceId = serviceId;
    this.clientConfig = clientConfig;
    this.metricsClient = new MetricsClient(clientConfig);
  }

  private ClientConfig getClientConfig() {
    ConnectionConfig connectionConfig = ConnectionConfig.builder(clientConfig.getConnectionConfig())
      .setNamespace(serviceId.getNamespace())
      .build();
    return new ClientConfig.Builder(clientConfig).setConnectionConfig(connectionConfig).build();
  }

  private ProgramClient getProgramClient() {
    return new ProgramClient(getClientConfig());
  }

  private ServiceClient getServiceClient() {
    return new ServiceClient(getClientConfig());
  }

  @Override
  public void setRunnableInstances(String runnable, int instances) {
    try {
      getProgramClient().setServiceRunnableInstances(serviceId.getApplicationId(), serviceId.getId(), runnable, instances);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getRequestedInstances(String runnableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getProvisionedInstances(String runnableName) {
    try {
      return getProgramClient().getServiceRunnableInstances(serviceId.getApplicationId(), serviceId.getId(), runnableName);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    try {
      getProgramClient().stop(serviceId.getApplicationId(), ProgramType.SERVICE, serviceId.getId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isRunning() {
    try {
      return "RUNNING".equals(getProgramClient().getStatus(serviceId.getApplicationId(), ProgramType.SERVICE, serviceId.getId()));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public URL getServiceURL() {
    try {
      return getServiceClient().getServiceURL(serviceId.getApplicationId(), serviceId.getId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public URL getServiceURL(long timeout, TimeUnit timeoutUnit) {
    try {
      // TODO: handle timeout
      return getServiceClient().getServiceURL(serviceId.getApplicationId(), serviceId.getId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public RuntimeMetrics getMetrics() {
    return metricsClient.getServiceMetrics(serviceId);
  }
}
