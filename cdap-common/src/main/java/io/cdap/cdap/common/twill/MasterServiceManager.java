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

package io.cdap.cdap.common.twill;

import io.cdap.cdap.proto.SystemServiceLiveInfo;
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
  default boolean isServiceEnabled() {
    // By default all the services are enabled. extending classes can override if the behavior should be different.
    return true;
  }

  /**
   * @return service description.
   */
  String getDescription();

  /**
   * Used to get the count of the instances of the CDAP Service that are currently running.
   *
   * @return the number of instances of the CDAP Service instances alive.
   */
  default int getInstances() {
    return 1;
  }

  /**
   * Set the number of instances of the CDAP service.
   *
   * @param instanceCount number of instances (should be greater than 0)
   * @return was the operation successful
   */
  default boolean setInstances(int instanceCount) {
    throw new UnsupportedOperationException("Setting number of instances is not supported.");
  }

  /**
   * Get the minimum instance count for the service.
   *
   * @return the required minimum number of instances of the CDAP Service.
   */
  default int getMinInstances() {
    return isServiceEnabled() ? 1 : 0;
  }

  /**
   * Get the maximum instance count for the service.
   *
   * @return the allowed maximum number of instances of the CDAP Service.
   */
  default int getMaxInstances() {
    return 1;
  }

  /**
   * Logging availability.
   *
   * @return true if logs are available.
   */
  default boolean isLogAvailable() {
    return true;
  }

  /**
   * Service's availability.
   *
   * @return true if service is available.
   */
  boolean isServiceAvailable();

  /**
   * Returns information about this system service runtime information. The information may not be available in
   * all runtime environment.
   */
  SystemServiceLiveInfo getLiveInfo();

  /**
   * Restart all instances of this service.
   */
  default void restartAllInstances() {
    throw new UnsupportedOperationException("Restart all instances is not supported.");
  }

  /**
   * Restart some instances of this service.
   *
   * @param instanceId the instance id to be restarted.
   * @param moreInstanceIds optional additional instance ids to be restarted.
   */
  default void restartInstances(int instanceId, int... moreInstanceIds) {
    throw new UnsupportedOperationException("Restart instances is not supported.");
  }

  /**
   * Update log levels for this service
   *
   * @param logLevels The {@link Map} contains the requested logger name and log level.
   */
  default void updateServiceLogLevels(Map<String, LogEntry.Level> logLevels) {
    throw new UnsupportedOperationException("Update service log levels is not supported.");
  }

  /**
   * Reset the log levels of the service.
   * All loggers will be reset to the level when the service started.
   *
   * @param loggerNames The set of logger names to be reset, if empty, all loggers will be reset.
   */
  default void resetServiceLogLevels(Set<String> loggerNames) {
    throw new UnsupportedOperationException("Reset service log levels is not supported.");
  }
}
