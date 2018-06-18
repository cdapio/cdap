/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.store;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Dataset for namespace metadata
 */
public final class NamespaceMDS extends MetadataStoreDataset {
  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);

  private static final String TYPE_NAMESPACE = "namespace";

  NamespaceMDS(Table table) {
    super(table);
  }

  public static NamespaceMDS getNamespaceMDS(DatasetContext datasetContext, DatasetFramework dsFramework) {
    try {
      Table table = DatasetsUtil.getOrCreateDataset(datasetContext, dsFramework, APP_META_INSTANCE_ID,
                                                    Table.class.getName(), DatasetProperties.EMPTY);
      return new NamespaceMDS(table);
    } catch (DatasetManagementException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void create(NamespaceMeta metadata) {
    write(getNamespaceKey(metadata.getName()), metadata);
  }

  @Nullable
  public NamespaceMeta get(NamespaceId id) {
    return getFirst(getNamespaceKey(id.getEntityName()), NamespaceMeta.class);
  }

  public void delete(NamespaceId id) {
    deleteAll(getNamespaceKey(id.getEntityName()));
  }

  public List<NamespaceMeta> list() {
    return list(getNamespaceKey(null), NamespaceMeta.class);
  }

  private MDSKey getNamespaceKey(@Nullable String name) {
    MDSKey.Builder builder = new MDSKey.Builder().add(TYPE_NAMESPACE);
    if (name != null) {
      builder.add(name);
    }
    return builder.build();
  }
}
