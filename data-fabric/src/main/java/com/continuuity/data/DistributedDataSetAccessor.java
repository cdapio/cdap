package com.continuuity.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseMetricsTableClient;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseMetricsTableManager;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableClient;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableManager;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DistributedDataSetAccessor extends AbstractDataSetAccessor {
  protected final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;

  @Inject
  public DistributedDataSetAccessor(@Named("HBaseOVCTableHandleCConfig") CConfiguration cConf,
                                    @Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                                    LocationFactory locationFactory)
    throws IOException {
    super(cConf);
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
  }

  @Override
  protected <T> T getOcTableClient(String name, ConflictDetection level) throws Exception {
    return (T) new HBaseOcTableClient(name, level, hConf);
  }

  @Override
  protected DataSetManager getOcTableManager() throws Exception {
    return new HBaseOcTableManager(cConf, hConf, locationFactory);
  }

  @Override
  protected <T> T getMetricsTableClient(String name) throws Exception {
    return (T) new HBaseMetricsTableClient(name, hConf);
  }

  @Override
  protected DataSetManager getMetricsTableManager() throws Exception {
    return new HBaseMetricsTableManager(cConf, hConf, locationFactory);
  }

  @Override
  protected Map<String, Class<?>> list(String prefix) throws Exception {
    Map<String, Class<?>> datasets = Maps.newHashMap();
    HBaseAdmin admin = new HBaseAdmin(hConf);
    for (HTableDescriptor tableDescriptor : admin.listTables()) {
      String tableName = Bytes.toString(tableDescriptor.getName());
      if (tableName.startsWith(prefix)) {
        datasets.put(tableName, OrderedColumnarTable.class);
      }
    }
    return datasets;
  }
}

