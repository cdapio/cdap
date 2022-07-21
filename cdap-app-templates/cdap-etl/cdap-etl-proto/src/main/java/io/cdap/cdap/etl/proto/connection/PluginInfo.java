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
 *
 */

package io.cdap.cdap.etl.proto.connection;

import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Plugin information inside a connection
 */
public class PluginInfo extends PluginMeta {

  private final String category;

  public PluginInfo(String name, String type, @Nullable String category, Map<String, String> properties,
                    ArtifactSelectorConfig artifact) {
    super(name, type, properties, artifact);
    this.category = category;
  }

  @Nullable
  public String getCategory() {
    return category;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    if (!super.equals(o)) {
      return false;
    }

    PluginInfo that = (PluginInfo) o;
    return Objects.equals(category, that.category);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), category);
  }
}
