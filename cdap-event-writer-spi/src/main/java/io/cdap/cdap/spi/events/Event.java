/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.spi.events;

import javax.annotation.Nullable;

/**
 * Interface that represents a CDAP event
 *
 * @param <T> Event detail type
 */
public interface Event<T> {

  /**
   * Returns {@link EventType} for the event
   *
   * @return {@link EventType}
   */
  EventType getType();

  /**
   * Returns the timestamp at which event was published
   *
   * @return long
   */
  long getPublishTime();

  /**
   * Returns the version string for this event
   *
   * @return String version
   */
  String getVersion();

  /**
   * Returns the CDAP instance name associated with this event or null
   *
   * @return String instance name or null
   */
  @Nullable
  String getInstanceName();

  /**
   * Returns the project name associated with this instance or null
   *
   * @return String project name or null
   */
  @Nullable
  default String getProjectName() {
    return null;
  }

  /**
   * Returns the object that holds specific details of the event
   *
   * @return event details
   */
  T getEventDetails();
}
