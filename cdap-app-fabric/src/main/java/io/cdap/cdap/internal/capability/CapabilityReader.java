/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import io.cdap.cdap.api.app.ApplicationSpecification;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CapabilityReader interface with methods based on current capability status
 */
public interface CapabilityReader {

  /**
   * Throws {@link CapabilityNotAvailableException} if all capabilities are not enabled
   *
   * @param capabilities
   * @return
   * @throws IOException
   */
  void checkAllEnabled(Collection<String> capabilities) throws IOException, CapabilityNotAvailableException;

  /**
   * Get the configuration map for the capabilities if present
   *
   * @param capabilities
   * @return
   * @throws IOException
   */
  Map<String, CapabilityConfig> getConfigs(Collection<String> capabilities) throws IOException;

  /**
   * Throws {@link CapabilityNotAvailableException} if all capabilities in the spec are not enabled
   *
   * @param appSpec
   * @return
   * @throws IOException
   */
  default void checkAllEnabled(ApplicationSpecification appSpec) throws IOException, CapabilityNotAvailableException {
    Set<String> capabilities = appSpec.getPlugins().entrySet().stream()
      .flatMap(pluginEntry -> pluginEntry.getValue().getPluginClass().getRequirements().getCapabilities().stream())
      .collect(Collectors.toSet());
    checkAllEnabled(capabilities);
  }
}
