/*
 * Copyright 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.data2.metadata.indexer.Indexer;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link MetadataStore} used in memory mode.
 */
public class NoOpMetadataStore implements MetadataStore {

  @Override
  public void setProperties(MetadataScope scope, Id.NamespacedId entityId, Map<String, String> properties) {
    // NO-OP
  }

  @Override
  public void setProperties(MetadataScope scope, Id.NamespacedId entityId, Map<String, String> properties,
                            Indexer indexer) {
    // NO-OP
  }

  @Override
  public void addTags(MetadataScope scope, Id.NamespacedId entityId, String... tagsToAdd) {
    // NO-OP
  }

  @Override
  public Set<MetadataRecord> getMetadata(Id.NamespacedId entityId) {
    return ImmutableSet.of(new MetadataRecord(entityId, MetadataScope.USER),
                           new MetadataRecord(entityId, MetadataScope.SYSTEM));
  }

  @Override
  public MetadataRecord getMetadata(MetadataScope scope, Id.NamespacedId entityId) {
    return new MetadataRecord(entityId, scope);
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, Set<Id.NamespacedId> entityIds) {
    return Collections.emptySet();
  }

  @Override
  public Map<String, String> getProperties(Id.NamespacedId entityId) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, Id.NamespacedId entityId) {
    return Collections.emptyMap();
  }

  @Override
  public Set<String> getTags(Id.NamespacedId entityId) {
    return Collections.emptySet();
  }

  @Override
  public Set<String> getTags(MetadataScope scope, Id.NamespacedId entityId) {
    return Collections.emptySet();
  }

  @Override
  public void removeMetadata(Id.NamespacedId entityId) {
    // NO-OP
  }

  @Override
  public void removeMetadata(MetadataScope scope, Id.NamespacedId entityId) {
    // NO-OP
  }

  @Override
  public void removeProperties(MetadataScope scope, Id.NamespacedId entityId) {
    // NO-OP
  }

  @Override
  public void removeProperties(MetadataScope scope, Id.NamespacedId entityId, String... keys) {
    // NO-OP
  }

  @Override
  public void removeTags(MetadataScope scope, Id.NamespacedId entityId) {
    // NO-OP
  }

  @Override
  public void removeTags(MetadataScope scope, Id.NamespacedId entityId, String... tagsToRemove) {
    // NO-OP
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(String namespaceId, String searchQuery) {
    return Collections.emptySet();
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(MetadataScope scope, String namespaceId, String searchQuery) {
    return Collections.emptySet();
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadataOnType(String namespaceId, String searchQuery,
                                                              MetadataSearchTargetType type) {
    return Collections.emptySet();
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadataOnType(MetadataScope scope, String namespaceId,
                                                              String searchQuery, MetadataSearchTargetType type) {
    return Collections.emptySet();
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(Set<Id.NamespacedId> entityIds, long timeMillis) {
    return ImmutableSet.<MetadataRecord>builder()
      .addAll(getSnapshotBeforeTime(MetadataScope.USER, entityIds, timeMillis))
      .addAll(getSnapshotBeforeTime(MetadataScope.SYSTEM, entityIds, timeMillis))
      .build();
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope, Set<Id.NamespacedId> entityIds,
                                                   long timeMillis) {
    ImmutableSet.Builder<MetadataRecord> builder = ImmutableSet.builder();
    for (Id.NamespacedId entityId : entityIds) {
      builder.add(new MetadataRecord(entityId, scope));
    }
    return builder.build();
  }

  @Override
  public void upgrade() {
    // NO-OP
  }
}
