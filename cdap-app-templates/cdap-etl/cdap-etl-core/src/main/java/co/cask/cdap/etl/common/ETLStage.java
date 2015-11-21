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

package co.cask.cdap.etl.common;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * ETL Stage Configuration.
 */
public final class ETLStage {
  private final String name;
  private final Plugin plugin;
  // TODO : can remove the following properties and clean up the constructor after UI support.
  private final Map<String, String> properties;
  // TODO : remove errorDatasetName after CDAP-4232 is implemented
  private final String errorDatasetName;

  public ETLStage(String name, Plugin plugin, @Nullable String errorDatasetName) {
    this.name = name;
    this.plugin = plugin;
    this.properties = plugin.getProperties();
    this.errorDatasetName = errorDatasetName;
  }

  public ETLStage(String name, Plugin plugin) {
    this(name, plugin, null);
  }

  public ETLStage getCompatibleStage(String stageName) {
    if (plugin != null) {
      return this;
    } else {
      return new ETLStage(stageName, new Plugin(name, properties), errorDatasetName);
    }
  }

  public String getName() {
    return name;
  }

  public Plugin getPlugin() {
    return plugin;
  }

  public String getErrorDatasetName() {
    return errorDatasetName;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("plugin", plugin.toString())
      .toString();
  }

}
