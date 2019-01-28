/*
 * Copyright 2015-2019 Cask Data, Inc.
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

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link MetadataStore} used in memory mode.
 */
public class NoOpMetadataStore implements MetadataStore {

  @Override
  public void replaceMetadata(MetadataScope scope, MetadataDataset.Record metadata,
                              Set<String> propertiesToKeep, Set<String> propertiesToPreserve) {
    // NO-OP
  }

  @Override
  public void addProperties(MetadataScope scope, MetadataEntity metadataEntity,
                            Map<String, String> properties) {
    // NO-OP
  }

  @Override
  public void addProperties(MetadataScope scope, Map<MetadataEntity, Map<String, String>> toUpdate) {
    // NO-OP
  }

  @Override
  public void addProperty(MetadataScope scope, MetadataEntity metadataEntity, String key, String value) {
    // NO-OP
  }

  @Override
  public void addTags(MetadataScope scope, MetadataEntity metadataEntity, Set<String> tagsToAdd) {
    // NO-OP
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity) {
    return ImmutableSet.of(new MetadataRecord(metadataEntity, MetadataScope.USER),
                           new MetadataRecord(metadataEntity, MetadataScope.SYSTEM));
  }

  @Override
  public MetadataRecord getMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    return new MetadataRecord(metadataEntity, scope);
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, Set<MetadataEntity> metadataEntities) {
    return Collections.emptySet();
  }

  @Override
  public Map<String, String> getProperties(MetadataEntity metadataEntity) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, MetadataEntity metadataEntity) {
    return Collections.emptyMap();
  }

  @Override
  public Set<String> getTags(MetadataEntity metadataEntity) {
    return Collections.emptySet();
  }

  @Override
  public Set<String> getTags(MetadataScope scope, MetadataEntity metadataEntity) {
    return Collections.emptySet();
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {
    // NO-OP
  }

  @Override
  public void removeMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    // NO-OP
  }

  @Override
  public void removeProperties(MetadataScope scope, MetadataEntity metadataEntity) {
    // NO-OP
  }

  @Override
  public void removeProperties(MetadataScope scope, MetadataEntity metadataEntity, Set<String> keys) {
    // NO-OP
  }

  @Override
  public void removeProperties(MetadataScope scope, Map<MetadataEntity, Set<String>> toRemove) {
    // NO-OP
  }

  @Override
  public void removeTags(MetadataScope scope, MetadataEntity metadataEntity) {
    // NO-OP
  }

  @Override
  public void removeTags(MetadataScope scope, MetadataEntity metadataEntity, Set<String> tagsToRemove) {
    // NO-OP
  }

  @Override
  public MetadataSearchResponse search(SearchRequest request) {
    return new MetadataSearchResponse(request.getSortInfo().toString(), request.getOffset(), request.getLimit(),
                                      request.getNumCursors(), 0, Collections.emptySet(), Collections.emptyList(),
                                      request.shouldShowHidden(), request.getEntityScopes());
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope, Set<MetadataEntity> metadataEntities,
                                                   long timeMillis) {
    ImmutableSet.Builder<MetadataRecord> builder = ImmutableSet.builder();
    for (MetadataEntity metadataEntity : metadataEntities) {
      builder.add(new MetadataRecord(metadataEntity, scope));
    }
    return builder.build();
  }

}
