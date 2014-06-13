package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset.lib.hbase.AbstractHBaseDataSetManager;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Dataset manager for HBase tables.
 */
public class HBaseOcTableManager extends AbstractHBaseDataSetManager {

  static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");

  private final CConfiguration conf;
  private LocationFactory locationFactory;

  @Inject
  public HBaseOcTableManager(CConfiguration conf, Configuration hConf,
                             LocationFactory locationFactory, HBaseTableUtil tableUtil) throws IOException {
    super(hConf, tableUtil);
    this.conf = conf;
    this.locationFactory = locationFactory;
  }

  @Override
  protected String getHBaseTableName(String name) {
    return HBaseTableUtil.getHBaseTableName(name);
  }

  @Override
  protected CoprocessorJar createCoprocessorJar() throws IOException {
    if (!conf.getBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE,
                        TxConstants.DataJanitor.DEFAULT_TX_JANITOR_ENABLE)) {
      return CoprocessorJar.EMPTY;
    }

    // create the jar for the data janitor coprocessor.
    Location jarDir = locationFactory.create(conf.get(Constants.CFG_HDFS_LIB_DIR));
    Class<? extends Coprocessor> dataJanitorClass = tableUtil.getTransactionDataJanitorClassForVersion();
    ImmutableList<Class<? extends Coprocessor>> coprocessors =
      ImmutableList.<Class<? extends Coprocessor>>of(dataJanitorClass);
    Location jarFile = HBaseTableUtil.createCoProcessorJar("table", jarDir, coprocessors);
    return new CoprocessorJar(coprocessors, jarFile);
  }

  @Override
  protected boolean upgradeTable(HTableDescriptor tableDescriptor, Properties properties) {
    HColumnDescriptor columnDescriptor = tableDescriptor.getFamily(DATA_COLUMN_FAMILY);

    boolean needUpgrade = false;
    if (columnDescriptor.getMaxVersions() < Integer.MAX_VALUE) {
      columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
      needUpgrade = true;
    }
    if (tableUtil.getBloomFilter(columnDescriptor) != HBaseTableUtil.BloomType.ROW) {
      tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);
      needUpgrade = true;
    }
    return needUpgrade;
  }

  @Override
  public boolean exists(String name) throws Exception {
    return getHBaseAdmin().tableExists(getHBaseTableName(name));
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
    // NOTE: we cannot limit number of versions as there's no hard limit on # of excluded from read txs
    columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);

    if (props != null) {
      String ttlProp = props.getProperty(TxConstants.PROPERTY_TTL);
      if (ttlProp != null) {
        int ttl = Integer.parseInt(ttlProp);
        if (ttl > 0) {
          columnDescriptor.setValue(TxConstants.PROPERTY_TTL, String.valueOf(ttl));
        }
      }
    }

    final HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    tableDescriptor.addFamily(columnDescriptor);
    CoprocessorJar coprocessorJar = createCoprocessorJar();

    for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
      addCoprocessor(tableDescriptor, coprocessor, coprocessorJar.getJarLocation());
    }
    tableUtil.createTableIfNotExists(getHBaseAdmin(), tableName, tableDescriptor);
  }

  @Override
  public void truncate(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(getHBaseTableName(name));
    HBaseAdmin admin = getHBaseAdmin();
    HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.createTable(tableDescriptor);
  }

  @Override
  public void drop(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(getHBaseTableName(name));
    HBaseAdmin admin = getHBaseAdmin();
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }
}
