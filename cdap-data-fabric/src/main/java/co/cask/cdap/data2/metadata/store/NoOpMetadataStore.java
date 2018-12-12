/*
 * Copyright 2015-2018 Cask Data, Inc.
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
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.proto.metadata.MetadataSearchResponseV2;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link MetadataStore} used in memory mode.
 */
public class NoOpMetadataStore implements MetadataStore {

  @Override
  public void setProperties(MetadataScope scope, MetadataEntity metadataEntity,
                            Map<String, String> properties) {
    // NO-OP
  }

  @Override
  public void setProperty(MetadataScope scope, MetadataEntity metadataEntity, String key, String value) {
    // NO-OP
  }

  @Override
  public void addTags(MetadataScope scope, MetadataEntity metadataEntity, Set<String> tagsToAdd) {
    // NO-OP
  }

  @Override
  public Set<MetadataRecordV2> getMetadata(MetadataEntity metadataEntity) {
    return ImmutableSet.of(new MetadataRecordV2(metadataEntity, MetadataScope.USER),
                           new MetadataRecordV2(metadataEntity, MetadataScope.SYSTEM));
  }

  @Override
  public MetadataRecordV2 getMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    return new MetadataRecordV2(metadataEntity, scope);
  }

  @Override
  public Set<MetadataRecordV2> getMetadata(MetadataScope scope, Set<MetadataEntity> metadataEntities) {
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
  public void removeTags(MetadataScope scope, MetadataEntity metadataEntity) {
    // NO-OP
  }

  @Override
  public void removeTags(MetadataScope scope, MetadataEntity metadataEntity, Set<String> tagsToRemove) {
    // NO-OP
  }

  @Override
  public MetadataSearchResponseV2 search(SearchRequest request) {
    return new MetadataSearchResponseV2(request.getSortInfo().toString(), request.getOffset(), request.getLimit(),
                                        request.getNumCursors(), 0, Collections.emptySet(), Collections.emptyList(),
                                        request.shouldShowHidden(), request.getEntityScopes());
  }

  @Override
  public Set<MetadataRecordV2> getSnapshotBeforeTime(MetadataScope scope, Set<MetadataEntity> metadataEntities,
                                                     long timeMillis) {
    ImmutableSet.Builder<MetadataRecordV2> builder = ImmutableSet.builder();
    for (MetadataEntity metadataEntity : metadataEntities) {
      builder.add(new MetadataRecordV2(metadataEntity, scope));
    }
    return builder.build();
  }

}
