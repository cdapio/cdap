package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import java.io.IOException;

/**
 * Dataset manager for HBase tables.
 */
public class HBaseOcTableManager implements DataSetManager {

  static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");

  private final HBaseAdmin admin;
  private final String tablePrefix;

  @Inject
  public HBaseOcTableManager(CConfiguration cConf, Configuration hConf)
    throws IOException {
    admin = new HBaseAdmin(hConf);
    tablePrefix = HBaseTableUtil.getTablePrefix(cConf);
  }

  private String getHBaseTableName(String name) {
    return HBaseTableUtil.getHBaseTableName(tablePrefix, name);
  }

  @Override
  public boolean exists(String name) throws Exception {
    return admin.tableExists(getHBaseTableName(name));
  }

  @Override
  public void create(String name) throws Exception {
    HTableDescriptor tableDescriptor = new HTableDescriptor(getHBaseTableName(name));
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(DATA_COLUMN_FAMILY);
    // todo: make stuff configurable
    // todo: using snappy compression for some reason breaks mini-hbase cluster (i.e. unit-test doesn't work)
//    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
    columnDescriptor.setMaxVersions(100);
    columnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW);
    tableDescriptor.addFamily(columnDescriptor);
    admin.createTable(tableDescriptor);
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
