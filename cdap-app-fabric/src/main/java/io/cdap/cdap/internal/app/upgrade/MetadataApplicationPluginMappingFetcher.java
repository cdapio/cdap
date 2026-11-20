/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.upgrade;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Fetcher for application to plugin mapping from the stored metadata.
 */
public class MetadataApplicationPluginMappingFetcher implements ApplicationPluginMappingFetcher {

  private static final String CONNECTOR_PROPERTY = "connector";
  private static final String PROPERTY_SEPARATOR = ":";

  private final MetadataAdmin metadataAdmin;

  @Inject
  public MetadataApplicationPluginMappingFetcher(MetadataAdmin metadataAdmin) {
    this.metadataAdmin = metadataAdmin;
  }

  @Override
  public List<ApplicationPluginMapping> fetchApplicationPluginMapping(NamespaceId namespaceId)
      throws Exception {
    String namespace = namespaceId.getNamespace();
    List<ApplicationPluginMapping> results = new ArrayList<>();
    results.addAll(fromSearchResponse(
        metadataAdmin.search(buildSearchRequest(MetadataScope.USER, namespaceId)),
        namespace));
    results.addAll(
        fromSearchResponse(
            metadataAdmin.search(buildSearchRequest(MetadataScope.SYSTEM, namespaceId)),
            namespace));
    return results;
  }

  private List<ApplicationPluginMapping> fromSearchResponse(SearchResponse response,
      String namespace) {
    List<ApplicationPluginMapping> results = new ArrayList<>();
    for (MetadataRecord result : response.getResults()) {
      MetadataEntity pluginEntity = result.getEntity();
      if (!MetadataEntity.PLUGIN.equalsIgnoreCase(pluginEntity.getType())) {
        continue;
      }
      PluginId pluginDetail = toPluginId(pluginEntity);
      String namespacePrefix = namespace + PROPERTY_SEPARATOR;
      // Pipelines are present as part of system scope.
      Map<String, String> pluginProperties = result.getMetadata()
          .getProperties(MetadataScope.SYSTEM);
      for (String propertyKey : pluginProperties.keySet()) {
        if (CONNECTOR_PROPERTY.equalsIgnoreCase(propertyKey) || !propertyKey
            .startsWith(namespacePrefix)) {
          continue;
        }
        String[] split = propertyKey.split(PROPERTY_SEPARATOR);
        results.add(
            new ApplicationPluginMapping(new ApplicationId(split[0], split[1]), pluginDetail));
      }
    }
    return results;
  }

  private PluginId toPluginId(MetadataEntity pluginEntity) {
    return new PluginId(pluginEntity.getValue(MetadataEntity.NAMESPACE),
        pluginEntity.getValue(MetadataEntity.ARTIFACT),
        pluginEntity.getValue(MetadataEntity.VERSION),
        pluginEntity.getValue(MetadataEntity.PLUGIN),
        pluginEntity.getValue(MetadataEntity.TYPE));
  }

  private SearchRequest buildSearchRequest(MetadataScope scope, NamespaceId namespaceId) {
    SearchRequest.Builder builder = SearchRequest.of("*").
        addType(MetadataEntity.PLUGIN).
        // Fetching all records is ok here since it will be equal to number of plugins.
        setLimit(Integer.MAX_VALUE);
    if (MetadataScope.SYSTEM == scope) {
      builder.addNamespace(scope.toString().toLowerCase());
    } else {
      builder.addNamespace(namespaceId.getNamespace());
    }
    return builder.build();
  }
}
