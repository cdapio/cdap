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

package co.cask.cdap.common.logging;

import java.util.Collection;
import java.util.Map;

/**
 * Defines LoggingContext interface so that we have at least somewhat structured logging context data and know how to
 * inject it on the logs writing end.
 */
public interface LoggingContext {
  /**
   * @return collection of system tags associated with this logging context
   */
  Collection<SystemTag> getSystemTags();

  /**
   * @return Map of tag name to system tag associated with this logging context
   */
  Map<String, SystemTag> getSystemTagsMap();

  // hack hack hack: time constraints
  /**
   * Returns the partition name that is used to group log messages of a component into one partition for collection.
   * The partition name must be consistent with the value returned as path fragment.
   * @return partition name.
   */
  String getLogPartition();

  /**
   * Returns the path fragment that will be part of the log file name. The grouping of log messages into partitions
   * should be consistent with the value returned as path fragment.
   * @return the path fragment that will be part of the log file name.
   * @param logBaseDir base dir for logs
   */
  String getLogPathFragment(String logBaseDir);

  /**
   * Defines the interface for the system tag associated with LoggingContext.
   */
  interface SystemTag {
    /**
     * @return tag name
     */
    String getName();

    /**
     * @return tag value
     */
    String getValue();
  }
}
