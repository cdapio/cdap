/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.ReactorDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceMDS;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetTypeMDS;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseOrderedTableModule;

import java.io.IOException;

/**
 * Utility for working with dataset metadata table.
 */
public class DatasetMetaTableUtil {
  public static final String META_TABLE_NAME = "datasets.type";
  public static final String INSTANCE_TABLE_NAME = "datasets.instance";

  private final DatasetFramework framework;

  public DatasetMetaTableUtil(DatasetFramework framework) {
    this.framework = framework;
  }

  public void init() throws DatasetManagementException {
    addTypes(framework);
  }

  public DatasetTypeMDS getTypeMetaTable() throws DatasetManagementException, IOException {
    return (DatasetTypeMDS) DatasetsUtil.getOrCreateDataset(framework, META_TABLE_NAME,
                                                            DatasetTypeMDS.class.getName(),
                                                            DatasetProperties.EMPTY, null, null);
  }

  public DatasetInstanceMDS getInstanceMetaTable() throws DatasetManagementException, IOException {
    return (DatasetInstanceMDS) DatasetsUtil.getOrCreateDataset(framework, INSTANCE_TABLE_NAME,
                                                                DatasetInstanceMDS.class.getName(),
                                                                DatasetProperties.EMPTY, null, null);
  }

  public void upgrade() throws Exception {
    DatasetsUtil.upgradeDataset(framework, META_TABLE_NAME, null);
    DatasetsUtil.upgradeDataset(framework, INSTANCE_TABLE_NAME, null);
  }

  /**
   * Sets up a {@link DatasetFramework} instance for standalone usage.  NOTE: should NOT be used by applications!!!
   */
  public static DatasetFramework createRegisteredDatasetFramework(DatasetDefinitionRegistryFactory registryFactory,
                                                                  CConfiguration cConf)
    throws DatasetManagementException, IOException {
    DatasetFramework mdsDatasetFramework =
      new NamespacedDatasetFramework(new InMemoryDatasetFramework(registryFactory),
                                     new ReactorDatasetNamespace(cConf, Namespace.SYSTEM));
    mdsDatasetFramework.addModule("orderedTable", new HBaseOrderedTableModule());
    addTypes(mdsDatasetFramework);
    mdsDatasetFramework.addInstance(DatasetTypeMDS.class.getName(),
                                    DatasetMetaTableUtil.META_TABLE_NAME, DatasetProperties.EMPTY);
    mdsDatasetFramework.addInstance(DatasetInstanceMDS.class.getName(),
                                    DatasetMetaTableUtil.INSTANCE_TABLE_NAME, DatasetProperties.EMPTY);
    return mdsDatasetFramework;

  }

  private static void addTypes(DatasetFramework framework) throws DatasetManagementException {
    framework.addModule("typeMDSModule", new SingleTypeModule(DatasetTypeMDS.class));
    framework.addModule("instanceMDSModule", new SingleTypeModule(DatasetInstanceMDS.class));
  }
}
