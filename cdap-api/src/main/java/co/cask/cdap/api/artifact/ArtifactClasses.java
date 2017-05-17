/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.plugin.PluginClass;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Classes contained in an artifact, such as plugin classes and application classes.
 */
@Beta
public final class ArtifactClasses {
  private final Set<ApplicationClass> apps;
  private final Set<PluginClass> plugins;

  private ArtifactClasses(Set<ApplicationClass> apps, Set<PluginClass> plugins) {
    this.apps = apps;
    this.plugins = plugins;
  }

  /**
   * get set of application classes
   * @return {@link Set<ApplicationClass>}
   */
  public Set<ApplicationClass> getApps() {
    return apps;
  }

  /**
   * get the set of plugin classes
   * @return {@link Set<PluginClass>}
   */
  public Set<PluginClass> getPlugins() {
    return plugins;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactClasses that = (ArtifactClasses) o;
    return Objects.equals(apps, that.apps) &&
      Objects.equals(plugins, that.plugins);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apps, plugins);
  }

  @Override
  public String toString() {
    return "ArtifactClasses{" +
      "apps=" + apps +
      ", plugins=" + plugins +
      '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to more easily add application and plugin classes, and in the future, program and dataset classes.
   */
  public static class Builder {

    private final Set<ApplicationClass> apps = new HashSet<>();
    private final Set<PluginClass> plugins = new HashSet<>();

    public Builder addApps(ApplicationClass... apps) {
      Collections.addAll(this.apps, apps);
      return this;
    }

    public Builder addApps(Iterable<ApplicationClass> apps) {
      for (ApplicationClass app : apps) {
        this.apps.add(app);
      }
      return this;
    }

    public Builder addApp(ApplicationClass app) {
      apps.add(app);
      return this;
    }

    public Builder addPlugins(PluginClass... plugins) {
      Collections.addAll(this.plugins, plugins);
      return this;
    }

    public Builder addPlugins(Iterable<PluginClass> plugins) {
      for (PluginClass plugin : plugins) {
        this.plugins.add(plugin);
      }
      return this;
    }

    public Builder addPlugin(PluginClass plugin) {
      plugins.add(plugin);
      return this;
    }

    public ArtifactClasses build() {
      return new ArtifactClasses(Collections.unmodifiableSet(apps),
                                 Collections.unmodifiableSet(plugins));
    }
  }
}
