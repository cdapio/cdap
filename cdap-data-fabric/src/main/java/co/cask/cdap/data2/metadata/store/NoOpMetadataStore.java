/*
 * Copyright 2015-2017 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.store;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link MetadataStore} used in memory mode.
 */
public class NoOpMetadataStore implements MetadataStore {

  @Override
  public void setProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId,
                            Map<String, String> properties) {
    // NO-OP
  }

  @Override
  public void setProperty(MetadataScope scope, NamespacedEntityId namespacedEntityId, String key, String value) {
    // NO-OP
  }

  @Override
  public void addTags(MetadataScope scope, NamespacedEntityId namespacedEntityId, String... tagsToAdd) {
    // NO-OP
  }

  @Override
  public Set<MetadataRecord> getMetadata(NamespacedEntityId namespacedEntityId) {
    return ImmutableSet.of(new MetadataRecord(namespacedEntityId, MetadataScope.USER),
                           new MetadataRecord(namespacedEntityId, MetadataScope.SYSTEM));
  }

  @Override
  public MetadataRecord getMetadata(MetadataScope scope, NamespacedEntityId namespacedEntityId) {
    return new MetadataRecord(namespacedEntityId, scope);
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, Set<NamespacedEntityId> namespacedEntityIds) {
    return Collections.emptySet();
  }

  @Override
  public Map<String, String> getProperties(NamespacedEntityId namespacedEntityId) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId) {
    return Collections.emptyMap();
  }

  @Override
  public Set<String> getTags(NamespacedEntityId namespacedEntityId) {
    return Collections.emptySet();
  }

  @Override
  public Set<String> getTags(MetadataScope scope, NamespacedEntityId namespacedEntityId) {
    return Collections.emptySet();
  }

  @Override
  public void removeMetadata(NamespacedEntityId namespacedEntityId) {
    // NO-OP
  }

  @Override
  public void removeMetadata(MetadataScope scope, NamespacedEntityId namespacedEntityId) {
    // NO-OP
  }

  @Override
  public void removeProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId) {
    // NO-OP
  }

  @Override
  public void removeProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId, String... keys) {
    // NO-OP
  }

  @Override
  public void removeTags(MetadataScope scope, NamespacedEntityId namespacedEntityId) {
    // NO-OP
  }

  @Override
  public void removeTags(MetadataScope scope, NamespacedEntityId namespacedEntityId, String... tagsToRemove) {
    // NO-OP
  }

  @Override
  public MetadataSearchResponse search(String namespaceId, String searchQuery,
                                       Set<EntityTypeSimpleName> types,
                                       SortInfo sort, int offset, int limit, int numCursors, String cursor,
                                       boolean showHidden, Set<EntityScope> entityScope) {
    return new MetadataSearchResponse(sort.toString(), offset, limit, numCursors, 0,
                                      Collections.<MetadataSearchResultRecord>emptySet(),
                                      Collections.<String>emptyList(), showHidden, entityScope);
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(Set<NamespacedEntityId> namespacedEntityIds, long timeMillis) {
    return ImmutableSet.<MetadataRecord>builder()
      .addAll(getSnapshotBeforeTime(MetadataScope.USER, namespacedEntityIds, timeMillis))
      .addAll(getSnapshotBeforeTime(MetadataScope.SYSTEM, namespacedEntityIds, timeMillis))
      .build();
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope, Set<NamespacedEntityId> namespacedEntityIds,
                                                   long timeMillis) {
    ImmutableSet.Builder<MetadataRecord> builder = ImmutableSet.builder();
    for (NamespacedEntityId namespacedEntityId : namespacedEntityIds) {
      builder.add(new MetadataRecord(namespacedEntityId, scope));
    }
    return builder.build();
  }

  @Override
  public void rebuildIndexes(MetadataScope scope, RetryStrategy retryStrategy) {
    // NO-OP
  }

  @Override
  public void deleteAllIndexes(MetadataScope scope) {
    // NO-OP
  }

  @Override
  public void createOrUpgrade(MetadataScope scope) throws DatasetManagementException, IOException {
    return;
  }

  @Override
  public void markUpgradeComplete(MetadataScope scope) throws DatasetManagementException, IOException {
    return;
  }

  @Override
  public boolean isUpgradeRequired(MetadataScope scope) throws DatasetManagementException {
    return false;
  }
}
