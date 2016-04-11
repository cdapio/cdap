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

package co.cask.cdap.etl.batch;

import co.cask.cdap.etl.spec.PluginSpec;

import java.util.Objects;

/**
 * Specification for a batch action.
 */
public class ActionSpec {
  private final String name;
  private final PluginSpec plugin;

  public ActionSpec(String name, PluginSpec plugin) {
    this.name = name;
    this.plugin = plugin;
  }

  public String getName() {
    return name;
  }

  public PluginSpec getPluginSpec() {
    return plugin;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ActionSpec that = (ActionSpec) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(plugin, that.plugin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, plugin);
  }

  @Override
  public String toString() {
    return "ActionSpec{" +
      "name='" + name + '\'' +
      ", plugin=" + plugin +
      '}';
  }
}
