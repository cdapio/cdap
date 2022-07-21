/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.logging.plugins;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.TriggeringPolicy;
import org.apache.twill.filesystem.Location;

/**
 * A TriggeringPolicy for locations
 */
public interface LocationTriggeringPolicy extends TriggeringPolicy<ILoggingEvent> {
  /**
   * Should roll-over be triggered at this time?
   *
   * @param event which {@link LocationTriggeringPolicy#setLocation(Location)}
   * @return true if a roll-over should occur.
   */
  boolean isTriggeringEvent(ILoggingEvent event);

  /**
   * set location of Rolling policy
   *
   * @param location location which can trigger roll over
   */
  void setLocation(Location location);

  /**
   * set {@link Location} size
   * @param size size of the location
   */
  void setActiveLocationSize(long size);
}
