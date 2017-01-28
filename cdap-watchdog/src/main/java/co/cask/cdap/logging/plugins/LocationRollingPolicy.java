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

package co.cask.cdap.logging.plugins;

import ch.qos.logback.core.rolling.RollingPolicy;
import org.apache.twill.filesystem.Location;

import java.io.Closeable;

/**
 * Location rolling policy
 */
public interface LocationRollingPolicy extends RollingPolicy {
   /**
   * Update Active file location
   *
   * @param activeFileLocation Current open file to be rolled over
   * @param closeable closeable to close location before renaming for rollover
   */
  void setLocation(Location activeFileLocation, Closeable closeable);
}
