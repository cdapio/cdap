package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Dataset manager for HBase tables.
 */
public class HBaseOcTableManager implements DataSetManager {

  static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");

  private final HBaseAdmin admin;

  @Inject
  public HBaseOcTableManager(Configuration hConf)
    throws IOException {
    admin = new HBaseAdmin(hConf);
  }

  private String getHBaseTableName(String name) {
    return HBaseTableUtil.getHBaseTableName(name);
  }

  @Override
  public boolean exists(String name) throws Exception {
    return admin.tableExists(getHBaseTableName(name));
  }

  @Override
  public void create(String name) throws Exception {
    HBaseTableUtil.createTableIfNotExists(admin, HBaseTableUtil.getHBaseTableName(name), DATA_COLUMN_FAMILY);
  }

  @Override
  public void truncate(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(getHBaseTableName(name));
    HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.createTable(tableDescriptor);
  }

  @Override
  public void drop(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(getHBaseTableName(name));
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }
}
