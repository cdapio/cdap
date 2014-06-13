package com.continuuity.data2.dataset2.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 *
 */
public class HBaseOrderedTableAdmin extends AbstractHBaseDataSetAdmin {
  static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");

  private final DatasetSpecification spec;
  // todo: datasets should not depend on continuuity configuration!
  private final CConfiguration conf;

  private final LocationFactory locationFactory;

  public HBaseOrderedTableAdmin(DatasetSpecification spec,
                                Configuration hConf,
                                HBaseTableUtil tableUtil,
                                CConfiguration conf,
                                LocationFactory locationFactory) throws IOException {
    super(spec.getName(), new HBaseAdmin(hConf), tableUtil);
    this.spec = spec;
    this.conf = conf;
    this.locationFactory = locationFactory;
  }

  @Override
  public boolean exists() throws IOException {
    return admin.tableExists(tableName);
  }

  @Override
  public void create() throws IOException {
    final String name = HBaseTableUtil.getHBaseTableName(tableName);

    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(DATA_COLUMN_FAMILY);
    // todo: make stuff configurable
    // NOTE: we cannot limit number of versions as there's no hard limit on # of excluded from read txs
    columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);

    String ttlProp = spec.getProperties().get(TxConstants.PROPERTY_TTL);
    if (ttlProp != null) {
      int ttl = Integer.parseInt(ttlProp);
      if (ttl > 0) {
        columnDescriptor.setValue(TxConstants.PROPERTY_TTL, String.valueOf(ttl));
      }
    }

    final HTableDescriptor tableDescriptor = new HTableDescriptor(name);
    tableDescriptor.addFamily(columnDescriptor);
    CoprocessorJar coprocessorJar = createCoprocessorJar();

    for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
      addCoprocessor(tableDescriptor, coprocessor, coprocessorJar.getJarLocation());
    }
    tableUtil.createTableIfNotExists(admin, name, tableDescriptor);

  }

  @Override
  public void truncate() throws IOException {
    byte[] tableName = Bytes.toBytes(this.tableName);
    HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.createTable(tableDescriptor);
  }

  @Override
  public void drop() throws IOException {
    byte[] tableName = Bytes.toBytes(this.tableName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Override
  public void close() throws IOException {
    admin.close();
  }

  @Override
  protected boolean upgradeTable(HTableDescriptor tableDescriptor) {
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
}
