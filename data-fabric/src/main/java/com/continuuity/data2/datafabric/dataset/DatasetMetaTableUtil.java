package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.NamespacedDatasetFramework;
import com.continuuity.data2.dataset2.module.lib.hbase.HBaseOrderedTableModule;

import java.io.IOException;

/**
 * Utility for working with dataset metadata table.
 */
public class DatasetMetaTableUtil {
  public static final String META_TABLE_NAME = "datasets.type";
  public static final String TABLE_TYPE = "orderedTable";
  public static final String INSTANCE_TABLE_NAME = "datasets.instance";

  private final DatasetFramework framework;

  public DatasetMetaTableUtil(DatasetFramework framework) {
    this.framework = framework;
  }

  public OrderedTable getTypeMetaTable() throws DatasetManagementException, IOException {
    OrderedTable table = DatasetsUtil.getOrCreateDataset(framework, META_TABLE_NAME, TABLE_TYPE,
                                                         DatasetProperties.EMPTY, null);
    return table;
  }

  public OrderedTable getInstanceMetaTable() throws DatasetManagementException, IOException {
    return DatasetsUtil.getOrCreateDataset(framework, INSTANCE_TABLE_NAME, TABLE_TYPE,
                                           DatasetProperties.EMPTY, null);
  }

  public void upgrade() throws Exception {
    DatasetsUtil.upgradeDataset(framework, META_TABLE_NAME, null);
    DatasetsUtil.upgradeDataset(framework, INSTANCE_TABLE_NAME, null);
  }

  /**
   * Sets up a {@link DatasetFramework} instance for standalone usage.  NOTE: should NOT be used by applications!!!
   */
  public static DatasetFramework createRegisteredDatasetFramework(DatasetDefinitionRegistry registry,
                                                                  CConfiguration cConf)
    throws DatasetManagementException, IOException {
    DatasetFramework mdsDatasetFramework =
      new NamespacedDatasetFramework(new InMemoryDatasetFramework(registry),
                                     new ReactorDatasetNamespace(cConf, DataSetAccessor.Namespace.SYSTEM));
    mdsDatasetFramework.addModule(DatasetMetaTableUtil.TABLE_TYPE, new HBaseOrderedTableModule());
    mdsDatasetFramework.addInstance(DatasetMetaTableUtil.TABLE_TYPE,
                                    DatasetMetaTableUtil.META_TABLE_NAME, DatasetProperties.EMPTY);
    mdsDatasetFramework.addInstance(DatasetMetaTableUtil.TABLE_TYPE,
                                    DatasetMetaTableUtil.INSTANCE_TABLE_NAME, DatasetProperties.EMPTY);
    return mdsDatasetFramework;

  }
}
