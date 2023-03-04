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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Plugin detail
 */
public class PluginDetail extends PluginMeta {

  private final Schema schema;

  public PluginDetail(String name, String type, Map<String, String> properties,
      ArtifactSelectorConfig artifact,
      @Nullable Schema schema) {
    super(name, type, properties, artifact);
    this.schema = schema;
  }

  @Nullable
  public Schema getSchema() {
    return schema;
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

    PluginDetail that = (PluginDetail) o;
    return Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), schema);
  }
}
