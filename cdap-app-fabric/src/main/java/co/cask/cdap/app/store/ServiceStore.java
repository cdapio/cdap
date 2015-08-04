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

package co.cask.cdap.app.store;

import co.cask.cdap.proto.RestartServiceInstancesStatus;
import com.google.common.util.concurrent.Service;

import javax.annotation.Nullable;

/**
 * Stores/Retrieves Information about System Services.
 */
public interface ServiceStore extends Service {

  /**
   * Get the service instance count.
   * @param serviceName Service Name.
   * @return Instance Count (can be null if no value was present for the given ServiceName).
   */
  @Nullable
  Integer getServiceInstance(String serviceName);

  /**
   * Set the service instance count.
   * @param serviceName Service Name.
   * @param instances Instance Count.
   */
  void setServiceInstance(String serviceName, int instances);

  /**
   * Update the service instances restart request.
   *
   * @param serviceName Service name to be restarted.
   * @param startTime Start time in Ms from Epoch.
   * @param endTime End time in Ms from Epoch.
   * @param isSuccess Whether or not the operation successful.
   * @param instanceId The instance Id to be restarted.
   */
  void setRestartInstanceRequest(String serviceName, long startTime, long endTime, boolean isSuccess,
                                  int instanceId);

  /**
   * Update the service instances restart request.
   *
   * @param serviceName Service name to be restarted.
   * @param startTime Start time in Ms from Epoch.
   * @param endTime End time in Ms from Epoch.
   * @param isSuccess Whether or not the operation successful.
   */
  void setRestartAllInstancesRequest(String serviceName, long startTime, long endTime, boolean isSuccess);

  /**
   * Get the latest service instances restart as JSON String.
   *
   * @param serviceName Service name for the restart record.
   * @return JSON string representation of latest restart instances for the service.
   * @throws IllegalStateException when restart request can not be found for the service name.
   */
  RestartServiceInstancesStatus getLatestRestartInstancesRequest(String serviceName) throws IllegalStateException;
}
