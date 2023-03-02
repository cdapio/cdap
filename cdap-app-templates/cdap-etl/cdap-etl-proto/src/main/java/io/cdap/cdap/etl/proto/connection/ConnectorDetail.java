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

import io.cdap.cdap.etl.api.connector.SampleType;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Response for the spec endpoint. The schema and properties are set on each available plugins. This
 * looks duplicated but it is our standard way on representing a plugin. Detail information about a
 * connector, contains all the plugins related to the connector and their versions.
 */
public class ConnectorDetail {

  private final Set<PluginDetail> relatedPlugins;
  private final Set<SampleType> supportedSampleTypes;

  public ConnectorDetail(Set<PluginDetail> relatedPlugins,
      @Nullable Set<SampleType> supportedSampleTypes) {
    this.relatedPlugins = relatedPlugins;
    if (supportedSampleTypes != null) {
      this.supportedSampleTypes = supportedSampleTypes;
    } else {
      this.supportedSampleTypes = new HashSet<>();
    }
  }

  public ConnectorDetail(Set<PluginDetail> relatedPlugins) {
    this(relatedPlugins, null);
  }

  public Set<PluginDetail> getRelatedPlugins() {
    return relatedPlugins;
  }

  public Set<SampleType> getSupportedSampleTypes() {
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

    ConnectorDetail that = (ConnectorDetail) o;
    return Objects.equals(relatedPlugins, that.relatedPlugins)
        && Objects.equals(supportedSampleTypes, that.supportedSampleTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relatedPlugins, supportedSampleTypes);
  }
}
