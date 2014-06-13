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
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DistributedDataSetAccessor extends AbstractDataSetAccessor {
  protected final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final HBaseTableUtil tableUtil;

  @Inject
  public DistributedDataSetAccessor(CConfiguration cConf,
                                    Configuration hConf,
                                    LocationFactory locationFactory,
                                    HBaseTableUtil tableUtil)
    throws IOException {
    super(cConf);
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.tableUtil = tableUtil;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected <T> T getOcTableClient(String name, ConflictDetection level, int ttl) throws Exception {
    return (T) new HBaseOcTableClient(name, level, ttl, hConf);
  }

  @Override
  protected DataSetManager getOcTableManager() throws Exception {
    return new HBaseOcTableManager(cConf, hConf, locationFactory, tableUtil);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected <T> T getMetricsTableClient(String name) throws Exception {
    return (T) new HBaseMetricsTableClient(name, hConf);
  }

  @Override
  protected DataSetManager getMetricsTableManager() throws Exception {
    return new HBaseMetricsTableManager(hConf, tableUtil);
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

