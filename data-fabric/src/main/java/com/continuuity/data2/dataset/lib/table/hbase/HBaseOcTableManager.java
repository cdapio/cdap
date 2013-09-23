package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import java.io.IOException;
import java.util.Properties;

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
    create(name, null);
  }

  @Override
  public void create(String name, Properties props) throws Exception {
    final String tableName = HBaseTableUtil.getHBaseTableName(name);

    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(DATA_COLUMN_FAMILY);
    // todo: make stuff configurable
    // todo: using snappy compression for some reason breaks mini-hbase cluster (i.e. unit-test doesn't work)
    //    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
    columnDescriptor.setMaxVersions(100);
    columnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW);

    // todo: find a better way to make this configurable
    if (props != null) {
      String ttlProp = props.getProperty(HBaseTableUtil.PROPERTY_TTL);
      if (ttlProp != null) {
        int ttl = Integer.parseInt(ttlProp);
        if (ttl > 0) {
          columnDescriptor.setTimeToLive(ttl);
        }
      }
    }

    final HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    tableDescriptor.addFamily(columnDescriptor);

    HBaseTableUtil.createTableIfNotExists(admin, tableName, tableDescriptor);
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
