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

package io.cdap.cdap.etl.api.connector;

import io.cdap.cdap.api.data.schema.Schema;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The connector spec contains all the properties based on the path and plugin config
 */
public class ConnectorSpec {
  // schema is null when the connector is unable to retrieve it from the resource
  private final Schema schema;
  private final Set<PluginSpec> relatedPlugins;
  private final Set<String> supportedSampleTypes;

  private ConnectorSpec(@Nullable Schema schema,
                        Set<PluginSpec> relatedPlugins,
                        Set<String> supportedSampleTypes) {
    this.schema = schema;
    this.relatedPlugins = relatedPlugins;
    if (supportedSampleTypes != null) {
      this.supportedSampleTypes = supportedSampleTypes;
    } else {
      this.supportedSampleTypes = new HashSet<String>();
    }
  }

  private ConnectorSpec(@Nullable Schema schema, Set<PluginSpec> relatedPlugins) {
    this(schema, relatedPlugins, null);
  }

  @Nullable
  public Schema getSchema() {
    return schema;
  }

  public Set<PluginSpec> getRelatedPlugins() {
    return relatedPlugins;
  }

  public Set<String> getSupportedSampleTypes() {
    return supportedSampleTypes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConnectorSpec that = (ConnectorSpec) o;
    return Objects.equals(schema, that.schema)
            && Objects.equals(relatedPlugins, that.relatedPlugins)
            && Objects.equals(supportedSampleTypes, that.supportedSampleTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, relatedPlugins, supportedSampleTypes);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link ConnectorSpec}
   */
  public static class Builder {
    private Schema schema;
    private Set<PluginSpec> relatedPlugins;
    private final Set<String> supportedSampleTypes;

    public Builder() {
      this.relatedPlugins = new HashSet<>();
      this.supportedSampleTypes = new HashSet<>();
    }

    public Builder setSchema(@Nullable Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder setRelatedPlugins(Set<PluginSpec> relatedPlugins) {
      this.relatedPlugins.clear();
      this.relatedPlugins.addAll(relatedPlugins);
      return this;
    }

    public Builder addRelatedPlugin(PluginSpec relatedPlugin) {
      this.relatedPlugins.add(relatedPlugin);
      return this;
    }

    public Builder setSupportedSampleTypes(Set<String> supportedSampleTypes) {
      this.supportedSampleTypes.clear();
      this.supportedSampleTypes.addAll(supportedSampleTypes);
      return this;
    }

    public Builder addSupportedSampleType(String sampleType) {
      this.supportedSampleTypes.add(sampleType);
      return this;
    }

    public ConnectorSpec build() {
      return new ConnectorSpec(schema, relatedPlugins, supportedSampleTypes);
    }
  }
}
