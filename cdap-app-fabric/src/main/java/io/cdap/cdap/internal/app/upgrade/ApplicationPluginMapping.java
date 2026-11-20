/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.upgrade;

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.PluginId;
import java.util.Objects;

/**
 * Mapping for the latest application version and its plugin.
 */
public class ApplicationPluginMapping {

  // Version less application Id.
  private final ApplicationId applicationId;
  private final PluginId pluginId;

  public ApplicationPluginMapping(ApplicationId applicationId, PluginId pluginId) {
    this.applicationId = applicationId;
    this.pluginId = pluginId;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public PluginId getPluginId() {
    return pluginId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ApplicationPluginMapping)) {
      return false;
    }
    ApplicationPluginMapping that = (ApplicationPluginMapping) o;
    return Objects.equals(applicationId, that.applicationId) && Objects.equals(
        pluginId, that.pluginId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(applicationId, pluginId);
  }
}
