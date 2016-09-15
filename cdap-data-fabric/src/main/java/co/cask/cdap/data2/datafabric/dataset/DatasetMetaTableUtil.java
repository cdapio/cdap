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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceMDS;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetTypeMDS;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

/**
 * Utility for working with dataset metadata table.
 */
public final class DatasetMetaTableUtil {
  public static final String META_TABLE_NAME = "datasets.type";
  public static final String INSTANCE_TABLE_NAME = "datasets.instance";

  public static final DatasetId META_TABLE_INSTANCE_ID = NamespaceId.SYSTEM.dataset(META_TABLE_NAME);
  public static final DatasetId INSTANCE_TABLE_INSTANCE_ID = NamespaceId.SYSTEM.dataset(INSTANCE_TABLE_NAME);

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by dataset service mds.
   *
   * @param datasetFramework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework datasetFramework) throws IOException, DatasetManagementException {
    for (Map.Entry<String, ? extends DatasetModule> entry : getModules().entrySet()) {
      // meta tables should be in the system namespace
      DatasetModuleId moduleId = NamespaceId.SYSTEM.datasetModule(entry.getKey());
      datasetFramework.addModule(moduleId, entry.getValue());
    }

    datasetFramework.addInstance(DatasetTypeMDS.class.getName(), NamespaceId.SYSTEM.dataset(META_TABLE_NAME),
                                 DatasetProperties.EMPTY);
    datasetFramework.addInstance(DatasetInstanceMDS.class.getName(), NamespaceId.SYSTEM.dataset(INSTANCE_TABLE_NAME),
                                 DatasetProperties.EMPTY);
  }

  /**
   * @return dataset modules used by dataset mds
   */
  public static Map<String, ? extends DatasetModule> getModules() {
    return ImmutableMap.of("typeMDSModule", new SingleTypeModule(DatasetTypeMDS.class),
                           "instanceMDSModule", new SingleTypeModule(DatasetInstanceMDS.class));
  }

  private DatasetMetaTableUtil() {
    // protect the constructor for util class.
  }
}
