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

package co.cask.cdap.test.remote;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.ServiceManager;
import com.google.common.base.Throwables;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RemoteServiceManager implements ServiceManager {

  private final RemoteApplicationManager.ProgramId serviceId;
  private final ServiceClient serviceClient;
  private final ProgramClient programClient;

  public RemoteServiceManager(RemoteApplicationManager.ProgramId serviceId, ClientConfig clientConfig) {
    this.serviceId = serviceId;
    this.serviceClient = new ServiceClient(clientConfig);
    this.programClient = new ProgramClient(clientConfig);
  }

  @Override
  public void setRunnableInstances(String runnable, int instances) {
    try {
      programClient.setServiceRunnableInstances(serviceId.getApplicationId(), serviceId.getRunnableId(),
                                                runnable, instances);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getRunnableInstances(String runnableName) {
    try {
      return programClient.getServiceRunnableInstances(serviceId.getApplicationId(), serviceId.getRunnableId(),
                                                       runnableName);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    try {
      programClient.stop(serviceId.getApplicationId(), ProgramType.SERVICE, serviceId.getRunnableId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isRunning() {
    try {
      return "RUNNING".equals(programClient.getStatus(serviceId.getApplicationId(),
                                                      ProgramType.SERVICE, serviceId.getRunnableId()));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public URL getServiceURL() {
    try {
      return serviceClient.getServiceURL(serviceId.getApplicationId(), serviceId.getRunnableId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public URL getServiceURL(long timeout, TimeUnit timeoutUnit) {
    try {
      // TODO: handle timeout?
      return serviceClient.getServiceURL(serviceId.getApplicationId(), serviceId.getRunnableId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
