/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Removes metadata for deleted datasets.
 */
final class DeletedDatasetMetadataRemover {
  private static final Logger LOG = LoggerFactory.getLogger(DeletedDatasetMetadataRemover.class);
  private final NamespaceStore nsStore;
  private final MetadataStore metadataStore;
  private final DatasetFramework dsFramework;

  DeletedDatasetMetadataRemover(NamespaceStore nsStore, MetadataStore metadataStore, DatasetFramework dsFramework) {
    this.nsStore = nsStore;
    this.metadataStore = metadataStore;
    this.dsFramework = dsFramework;
  }

  void remove() throws DatasetManagementException, BadRequestException {
    List<DatasetId> removedDatasets = new ArrayList<>();
    for (NamespaceMeta namespaceMeta : nsStore.list()) {
      Set<MetadataSearchResultRecord> searchResults =
        metadataStore.search(namespaceMeta.getName(), "*", EnumSet.of(EntityTypeSimpleName.DATASET),
                             SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 0, null, false,
                             EnumSet.allOf(EntityScope.class)).getResults();
      for (MetadataSearchResultRecord searchResult : searchResults) {
        NamespacedEntityId entityId = searchResult.getEntityId();
        Preconditions.checkState(entityId instanceof DatasetId,
                                 "Since search was filtered for %s, expected result to be a %s, but got a %s",
                                 EntityTypeSimpleName.DATASET, DatasetId.class.getSimpleName(),
                                 entityId.getClass().getName());
        DatasetId datasetInstance = (DatasetId) entityId;
        if (!dsFramework.hasInstance(datasetInstance)) {
          metadataStore.removeMetadata(datasetInstance);
          removedDatasets.add(datasetInstance);
        }
      }
    }
    if (removedDatasets.isEmpty()) {
      LOG.debug("Deleted datasets with metadata not found. No metadata removal necessary.");
    } else {
      LOG.info("Removed metadata for the following deleted datasets: {}.", removedDatasets);
    }
  }
}
