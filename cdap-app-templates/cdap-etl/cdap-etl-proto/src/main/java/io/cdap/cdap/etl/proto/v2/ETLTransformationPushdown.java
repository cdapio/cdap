/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.proto.v2;

import java.util.Objects;

/**
 * Class used to represent the configuration properties that are needed to configure Transformation
 * Pushdown.
 */
public class ETLTransformationPushdown {

  private final ETLPlugin plugin;

  /**
   * Creates new instance.
   *
   * @param plugin the SQL Engine configuration
   */
  public ETLTransformationPushdown(ETLPlugin plugin) {
    this.plugin = plugin;
  }

  public ETLPlugin getPlugin() {
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
    ETLTransformationPushdown that = (ETLTransformationPushdown) o;
    return Objects.equals(plugin, that.plugin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(plugin);
  }

  @Override
  public String toString() {
    return "ETLTransformationPushdown{"
        + "plugin=" + plugin
        + '}';
  }
}
