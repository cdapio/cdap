/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.environment;

import org.apache.twill.filesystem.LocationFactory;

import java.util.Map;

/**
 * Context object available to {@link MasterEnvironment} for access to CDAP resources.
 */
public interface MasterEnvironmentContext {

  /**
   * Returns the {@link LocationFactory} used by the CDAP.
   */
  LocationFactory getLocationFactory();

  /**
   * Returns a {@link Map} that contains all CDAP configurations.
   */
  Map<String, String> getConfigurations();
}
