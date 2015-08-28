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
 * A container class for holding plugin information.
 */
public final class Plugin {
  private final String artifactName;
  private final ArtifactVersion artifactVersion;
  private final Boolean isSystem;
  private final URI locationURI;
  private final PluginClass pluginClass;
  private final PluginProperties properties;

  public Plugin(String artifactName, ArtifactVersion artifactVersion, boolean isSystem, URI locationURI,
                PluginClass pluginClass, PluginProperties properties) {
    this.artifactName = artifactName;
    this.artifactVersion = artifactVersion;
    this.isSystem = isSystem;
    this.locationURI = locationURI;
    this.pluginClass = pluginClass;
    this.properties = properties;
  }

  public String getArtifactName() {
    return artifactName;
  }

  /**
   * @return {@link ArtifactVersion}
   */
  public ArtifactVersion getArtifactVersion() {
    return artifactVersion;
  }

  /**
   * @return true if the plugin is part of a system artifact
   */
  public boolean isSystem() {
    return isSystem;
  }

  /**
   * @retun location of plugin artifact
   */
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
    return Objects.equals(artifactName, that.artifactName)
      && Objects.equals(artifactVersion, that.artifactVersion)
      && Objects.equals(isSystem, that.isSystem)
      && Objects.equals(locationURI, that.locationURI)
      && Objects.equals(pluginClass, that.pluginClass)
      && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(artifactName, artifactVersion, isSystem, locationURI, pluginClass, properties);
  }

  @Override
  public String toString() {
    return "AdapterPlugin{" +
      "artifactName=" + artifactName +
      ",artifactVersion=" + artifactVersion +
      ",isSystem=" + isSystem +
      ",locationURI=" + locationURI +
      ",pluginClass=" + pluginClass +
      ",properties=" + properties +
      '}';
  }
}
