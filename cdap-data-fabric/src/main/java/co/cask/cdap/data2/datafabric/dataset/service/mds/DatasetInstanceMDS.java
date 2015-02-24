/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.mds;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Dataset instances metadata store
 */
public final class DatasetInstanceMDS extends MetadataStoreDataset {
  /**
   * Prefix for rows containing instance info.
   * NOTE: even though we don't have to have it now we may want to store different type of data in one table, so
   *       the prefix may help us in future
   */
  private static final String INSTANCE_PREFIX = "i_";

  public DatasetInstanceMDS(DatasetSpecification spec, @EmbeddedDataset("") Table table) {
    super(table);
  }

  @Nullable
  public DatasetSpecification get(Id.DatasetInstance datasetInstanceId) {
    return get(getInstanceKey(datasetInstanceId.getNamespace(), datasetInstanceId.getId()),
               DatasetSpecification.class);
  }

  public void write(Id.Namespace namespaceId, DatasetSpecification instanceSpec) {
    write(getInstanceKey(namespaceId, instanceSpec.getName()), instanceSpec);
  }

  public boolean delete(Id.DatasetInstance datasetInstanceId) {
    if (get(datasetInstanceId) == null) {
      return false;
    }
    deleteAll(getInstanceKey(datasetInstanceId.getNamespace(), datasetInstanceId.getId()));
    return true;
  }

  public Collection<DatasetSpecification> getAll(Id.Namespace namespaceId) {
    Map<MDSKey, DatasetSpecification> instances = listKV(getInstanceKey(namespaceId), DatasetSpecification.class);
    return instances.values();
  }

  public Collection<DatasetSpecification> getByTypes(Id.Namespace namespaceId, Set<String> typeNames) {
    List<DatasetSpecification> filtered = Lists.newArrayList();

    for (DatasetSpecification spec : getAll(namespaceId)) {
      if (typeNames.contains(spec.getType())) {
        filtered.add(spec);
      }
    }

    return filtered;
  }

  private MDSKey getInstanceKey(Id.Namespace namespaceId) {
    return getInstanceKey(namespaceId, null);
  }

  private MDSKey getInstanceKey(Id.Namespace namespaceId, @Nullable String instanceName) {
    MDSKey.Builder builder = new MDSKey.Builder().add(INSTANCE_PREFIX).add(namespaceId.getId());
    if (instanceName != null) {
      builder.add(instanceName);
    }
    return builder.build();
  }
}
