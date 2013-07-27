package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import java.io.IOException;

/**
 *
 */
public class HBaseOcTableManager implements DataSetManager {
  static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");

  private final HBaseAdmin admin;

  @Inject
  public HBaseOcTableManager(Configuration hConf)
    throws IOException {
    this.admin = new HBaseAdmin(hConf);
  }


  @Override
  public boolean exists(String name) throws Exception {
    return admin.tableExists(name);
  }

  @Override
  public void create(String name) throws Exception {
    HTableDescriptor tableDescriptor = new HTableDescriptor(name);
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(DATA_COLUMN_FAMILY);
    // todo: make stuff configurable
//    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
    columnDescriptor.setMaxVersions(100);
    columnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW);
    tableDescriptor.addFamily(columnDescriptor);
    admin.createTable(tableDescriptor);
  }

  @Override
  public void truncate(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(name);
    HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.createTable(tableDescriptor);
  }

  @Override
  public void drop(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(name);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }
}
