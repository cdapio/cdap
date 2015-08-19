/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.artifact;

import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginProperties;

import java.net.URI;
import java.util.Objects;

/**
 * A container class for holding plugin information for an adapter instance.
 */
public final class Plugin {
  private final String pluginName;
  private final ArtifactVersion artifactVersion;
  private final Boolean isSystem;
  private final URI locationURI;
  private final PluginClass pluginClass;
  private final PluginProperties properties;

  public Plugin(String pluginName, ArtifactVersion artifactVersion, boolean isSystem, URI locationURI,
                PluginClass pluginClass, PluginProperties properties) {
    this.pluginName = pluginName;
    this.artifactVersion = artifactVersion;
    this.isSystem = isSystem;
    this.locationURI = locationURI;
    this.pluginClass = pluginClass;
    this.properties = properties;
  }

  public String getPluginName() {
    return pluginName;
  }

  public ArtifactVersion getArtifactVersion() {
    return artifactVersion;
  }

  public boolean isSystem() {
    return isSystem;
  }

  public URI getLocationURI() {
    return locationURI;
  }

  /**
   * Returns the plugin class information.
   */
  public PluginClass getPluginClass() {
    return pluginClass;
  }

  /**
   * Returns the set of properties available for the plugin when the adapter was created.
   */
  public PluginProperties getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Plugin that = (Plugin) o;

    return pluginName.equals(that.pluginName)
      && artifactVersion.equals(that.artifactVersion)
      && isSystem.equals(that.isSystem)
      && locationURI.equals(that.locationURI)
      && pluginClass.equals(that.pluginClass)
      && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pluginName, artifactVersion, isSystem, locationURI, pluginClass, properties);
  }

  @Override
  public String toString() {
    return "AdapterPlugin{" +
      "pluginName=" + pluginName +
      "artifactVersion=" + artifactVersion +
      "isSystem=" + isSystem +
      "locationURI=" + locationURI +
      "pluginClass=" + pluginClass +
      ", properties=" + properties +
      '}';
  }
}
