/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.plugin.Plugin;
import org.apache.twill.filesystem.Location;

/**
 * Class for holding a {@link Plugin} information and the artifact location of that plugin.
 */
public final class PluginWithLocation {

  private final Plugin plugin;
  private final Location artifactLocation;

  public PluginWithLocation(Plugin plugin, Location artifactLocation) {
    this.plugin = plugin;
    this.artifactLocation = artifactLocation;
  }

  public Plugin getPlugin() {
    return plugin;
  }

  public Location getArtifactLocation() {
    return artifactLocation;
  }
}
