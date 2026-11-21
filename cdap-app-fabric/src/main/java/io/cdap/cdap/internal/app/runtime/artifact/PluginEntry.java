/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.api.plugin.PluginClass;

/**
 * Represents a plugin entry with its artifact descriptor and plugin class.
 */
public class PluginEntry {
  private final ArtifactDescriptor descriptor;
  private final PluginClass pluginClass;

  public PluginEntry(ArtifactDescriptor descriptor, PluginClass pluginClass) {
    this.descriptor = descriptor;
    this.pluginClass = pluginClass;
  }

  public ArtifactDescriptor getDescriptor() {
    return descriptor;
  }

  public PluginClass getPluginClass() {
    return pluginClass;
  }
}
