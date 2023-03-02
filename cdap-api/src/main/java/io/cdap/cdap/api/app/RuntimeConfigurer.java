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
 *
 */

package io.cdap.cdap.api.app;

import io.cdap.cdap.api.ServiceDiscoverer;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The runtime configurer that can be got when the app is reconfigured before the actual program
 * run
 */
public interface RuntimeConfigurer extends ServiceDiscoverer {

  /**
   * @return A map of user argument key and value
   */
  Map<String, String> getRuntimeArguments();

  /**
   * @return The app spec generated when the app is initially deployed, null if there is no such
   *     spec, for example, for preview run, there is no existing app spec
   *
   *     This is deprecated, Use {@link ApplicationConfigurer#getDeployedApplicationSpec()}
   */
  @Nullable
  @Deprecated
  ApplicationSpecification getDeployedApplicationSpec();
}
