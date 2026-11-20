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

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.app.upgrade.UpgradeManager;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.proto.upgrade.ApplicationUpgradeDetail;
import io.cdap.cdap.proto.upgrade.ArtifactUpgradeDetail;
import io.cdap.cdap.proto.upgrade.PluginUpgradeDetail;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;

/**
 * Default implementation of {@link UpgradeManager}.
 */
public class DefaultUpgradeManager implements UpgradeManager {

  private final ApplicationPluginMappingFetcher mappingFetcher;
  private final ArtifactRepository artifactRepository;
  private final Store store;

  @Inject
  public DefaultUpgradeManager(ApplicationPluginMappingFetcher mappingFetcher,
      ArtifactRepository artifactRepository, Store store) {
    this.mappingFetcher = mappingFetcher;
    this.artifactRepository = artifactRepository;
    this.store = store;
  }

  @Override
  public List<ApplicationUpgradeDetail> listUpgrades(NamespaceId namespace) throws Exception {
    List<ApplicationPluginMapping> applicationPluginMappings =
        mappingFetcher.fetchApplicationPluginMapping(namespace);
    Map<String, List<PluginId>> applicationToPluginListMap = applicationPluginMappings.stream()
        .collect(Collectors.groupingBy(x -> x.getApplicationId().getApplication(),
            Collectors.mapping(ApplicationPluginMapping::getPluginId, Collectors.toList())));
    Map<String, ArtifactId> applicationIdArtifactIdMap = fetchApplicationArtifactMap(
        namespace);
    Map<String, Optional<ArtifactId>> artifactToLatestVersionMap = artifactToLatestVersionMap(
        namespace);
    return toApplicationUpgradeDetails(namespace, applicationToPluginListMap,
        artifactToLatestVersionMap, applicationIdArtifactIdMap);
  }

  private Map<String, ArtifactId> fetchApplicationArtifactMap(NamespaceId namespace) {
    ScanApplicationsRequest scanAppRequest = ScanApplicationsRequest.builder()
        .setNamespaceId(namespace).setLatestOnly(true).build();
    Map<String, ArtifactId> results = new HashMap<>();
    store.scanApplications(scanAppRequest, Integer.MAX_VALUE, (app, meta) -> {
      results.put(app.getApplication(), meta.getSpec().getArtifactId());
    });
    return results;
  }

  private Map<String, Optional<ArtifactId>> artifactToLatestVersionMap(NamespaceId namespace)
      throws Exception {
    List<ArtifactSummary> artifactSummaries = artifactRepository.getArtifactSummaries(
        namespace, true);
    // Get max version for each artifactID.
    return artifactSummaries.stream().
        map(x -> new ArtifactId(x.getName(), new ArtifactVersion(x.getVersion()), x.getScope())).
        collect(Collectors.groupingBy(ArtifactId::getName,
            Collectors.maxBy(Comparator.comparing(ArtifactId::getVersion))));
  }

  private List<ApplicationUpgradeDetail> toApplicationUpgradeDetails(NamespaceId namespaceId,
      Map<String, List<PluginId>> appToPluginsMap,
      Map<String, Optional<ArtifactId>> artifactToLatestVersionMap,
      Map<String, ArtifactId> applicationIdArtifactIdMap) throws ApplicationNotFoundException {
    List<ApplicationUpgradeDetail> results = new ArrayList<>();
    for (Entry<String, List<PluginId>> entry : appToPluginsMap.entrySet()) {
      String appName = entry.getKey();
      List<PluginUpgradeDetail> pluginUpgradeDetails = toPluginUpgradeDetails(entry.getValue(),
          artifactToLatestVersionMap);
      ArtifactId currentApplicationArtifact = applicationIdArtifactIdMap.get(appName);
      if (currentApplicationArtifact == null) {
        throw new ApplicationNotFoundException(
            new ApplicationId(namespaceId.getNamespace(), appName));
      }
      results.add(
          new ApplicationUpgradeDetail(appName,
              toArtifactUpgradeDetails(currentApplicationArtifact, artifactToLatestVersionMap),
              pluginUpgradeDetails));
    }
    return results;
  }

  private ArtifactUpgradeDetail toArtifactUpgradeDetails(ArtifactId current,
      Map<String, Optional<ArtifactId>> artifactToLatestVersionMap) {
    ArtifactId latest = artifactToLatestVersionMap.getOrDefault(current.getName(), Optional.empty())
        .orElse(current);
    return new ArtifactUpgradeDetail(current.getName(), current.getVersion().getVersion(),
        latest.getVersion().getVersion());
  }

  private List<PluginUpgradeDetail> toPluginUpgradeDetails(List<PluginId> pluginIdList,
      Map<String, Optional<ArtifactId>> artifactToLatestVersionMap) {
    return pluginIdList.stream().map(
        x -> {
          ArtifactUpgradeDetail artifactUpgradeDetail = toArtifactUpgradeDetails(
              x.getParent().toApiArtifactId(),
              artifactToLatestVersionMap);
          return new PluginUpgradeDetail(artifactUpgradeDetail, x.getPlugin(), x.getType());
        }).collect(
        Collectors.toList());
  }

}
