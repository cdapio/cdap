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

package co.cask.cdap.common.twill;

import co.cask.cdap.proto.SystemServiceLiveInfo;
import org.apache.twill.api.logging.LogEntry;

import java.util.Map;
import java.util.Set;

/**
 * Interface that defines a set of methods that will be used for management of CDAP Services.
 * Each individual service must provide an implementation.
 */
public interface MasterServiceManager {

  /**
   * @return true if the configured to be available, false otherwise.
   */
  boolean isServiceEnabled();

  /**
   * @return service description.
   */
  String getDescription();

  /**
   * Used to get the count of the instances of the CDAP Service that are currently running.
   *
   * @return the number of instances of the CDAP Service instances alive.
   */
  int getInstances();

  /**
   * Set the number of instances of the CDAP service.
   *
   * @param instanceCount number of instances (should be greater than 0)
   * @return was the operation successful
   */
  boolean setInstances(int instanceCount);

  /**
   * Get the minimum instance count for the service.
   *
   * @return the required minimum number of instances of the CDAP Service.
   */
  int getMinInstances();

  /**
   * Get the maximum instance count for the service.
   *
   * @return the allowed maximum number of instances of the CDAP Service.
   */
  int getMaxInstances();

  /**
   * Logging availability.
   *
   * @return true if logs are available.
   */
  boolean isLogAvailable();

  /**
   * Possible to check the status of the service.
   *
   * @return true if the status of the service can be checked.
   */
  boolean canCheckStatus();

  /**
   * Service's availability.
   *
   * @return true if service is available.
   */
  boolean isServiceAvailable();

  SystemServiceLiveInfo getLiveInfo();

  //TODO: Add method to get the metrics name to get event rate on UI

  /**
   * Restart all instances of this service.
   */
  void restartAllInstances();

  /**
   * Restart some instances of this service.
   *
   * @param instanceId the instance id to be restarted.
   * @param moreInstanceIds optional additional instance ids to be restarted.
   */
  void restartInstances(int instanceId, int... moreInstanceIds);

  /**
   * Update log levels for this service
   *
   * @param logLevels The {@link Map} contains the requested logger name and log level.
   */
  void updateServiceLogLevels(Map<String, LogEntry.Level> logLevels);

  /**
   * Reset the log levels of the service.
   * The log levels will be reset back to when the service was launched.
   *
   * @param loggerNames The set of logger names to be reset, if empty, all log levels will be reset.
   */
  void resetServiceLogLevels(Set<String> loggerNames);
}
