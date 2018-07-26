/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.indexer;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.id.EntityId;

import java.util.HashSet;
import java.util.Set;

/**
 * Indexer used to index a {@link MetadataEntity} with it's parent. The parent is determined by
 * {@link EntityId#getSelfOrParentEntityId(MetadataEntity)}.
 */
public class ParentIndexer implements Indexer {

  public static final String PARENT_KEY = "parent_entity";

  @Override
  public Set<String> getIndexes(MetadataEntry entry) {
    Set<String> indexes = new HashSet<>();
    EntityId selfOrParentEntityId = EntityId.getSelfOrParentEntityId(entry.getMetadataEntity());
    indexes.add(PARENT_KEY + MetadataDataset.KEYVALUE_SEPARATOR + selfOrParentEntityId);
    return indexes;
  }

  @Override
  public SortInfo.SortOrder getSortOrder() {
    return SortInfo.SortOrder.WEIGHTED;
  }
}
