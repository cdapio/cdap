package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.service.mds.DatasetInstanceMDS;
import com.continuuity.data2.datafabric.dataset.service.mds.DatasetTypeMDS;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.NamespacedDatasetFramework;
import com.continuuity.data2.dataset2.SingleTypeModule;
import com.continuuity.data2.dataset2.module.lib.hbase.HBaseOrderedTableModule;

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
                                                            DatasetProperties.EMPTY, null);
  }

  public DatasetInstanceMDS getInstanceMetaTable() throws DatasetManagementException, IOException {
    return (DatasetInstanceMDS) DatasetsUtil.getOrCreateDataset(framework, INSTANCE_TABLE_NAME,
                                                                DatasetInstanceMDS.class.getName(),
                                                                DatasetProperties.EMPTY, null);
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
                                     new ReactorDatasetNamespace(cConf, DataSetAccessor.Namespace.SYSTEM));
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
