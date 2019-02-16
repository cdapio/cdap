/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
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
  static final String INSTANCE_PREFIX = "i_";

  public DatasetInstanceMDS(DatasetSpecification spec, @EmbeddedDataset("") Table table) {
    super(table);
  }

  @Nullable
  public DatasetSpecification get(DatasetId datasetInstanceId) {
    return get(getInstanceKey(datasetInstanceId.getParent(), datasetInstanceId.getEntityName()),
               DatasetSpecification.class);
  }

  public void write(NamespaceId namespaceId, DatasetSpecification instanceSpec) {
    write(getInstanceKey(namespaceId, instanceSpec.getName()), instanceSpec);
  }

  public boolean delete(DatasetId datasetInstanceId) {
    if (get(datasetInstanceId) == null) {
      return false;
    }
    deleteAll(getInstanceKey(datasetInstanceId.getParent(), datasetInstanceId.getEntityName()));
    return true;
  }

  public Collection<DatasetSpecification> getAll(NamespaceId namespaceId) {
    Predicate<DatasetSpecification> localDatasetFilter = input -> input != null
      && !Boolean.parseBoolean(input.getProperty(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY));
    return getAll(namespaceId, localDatasetFilter);
  }

  public Collection<DatasetSpecification> get(NamespaceId namespaceId, final Map<String, String> properties) {
    Predicate<DatasetSpecification> propertyFilter = input -> input != null
      && Maps.difference(properties, input.getProperties()).entriesOnlyOnLeft().isEmpty();

    return getAll(namespaceId, propertyFilter);
  }

  private Collection<DatasetSpecification> getAll(NamespaceId namespaceId, Predicate<DatasetSpecification> filter) {
    Map<MDSKey, DatasetSpecification> instances = listKV(getInstanceKey(namespaceId), null, DatasetSpecification.class,
                                                         Integer.MAX_VALUE, filter);
    return instances.values();
  }

  public Collection<DatasetSpecification> getByTypes(NamespaceId namespaceId, Set<String> typeNames) {
    List<DatasetSpecification> filtered = Lists.newArrayList();

    for (DatasetSpecification spec : getAll(namespaceId)) {
      if (typeNames.contains(spec.getType())) {
        filtered.add(spec);
      }
    }

    return filtered;
  }

  private MDSKey getInstanceKey(NamespaceId namespaceId) {
    return getInstanceKey(namespaceId, null);
  }

  private MDSKey getInstanceKey(NamespaceId namespaceId, @Nullable String instanceName) {
    MDSKey.Builder builder = new MDSKey.Builder().add(INSTANCE_PREFIX).add(namespaceId.getEntityName());
    if (instanceName != null) {
      builder.add(instanceName);
    }
    return builder.build();
  }
}
