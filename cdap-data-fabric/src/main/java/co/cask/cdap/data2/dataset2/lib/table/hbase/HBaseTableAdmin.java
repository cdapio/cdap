/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.tephra.TxConstants;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 *
 */
public class HBaseTableAdmin extends AbstractHBaseDataSetAdmin {
  public static final String PROPERTY_SPLITS = "hbase.splits";
  static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");
  private static final Gson GSON = new Gson();

  private final DatasetSpecification spec;
  // todo: datasets should not depend on cdap configuration!
  private final CConfiguration conf;

  private final LocationFactory locationFactory;

  public HBaseTableAdmin(DatasetSpecification spec,
                         Configuration hConf,
                         HBaseTableUtil tableUtil,
                         CConfiguration conf,
                         LocationFactory locationFactory) throws IOException {
    super(spec.getName(), hConf, tableUtil);
    this.spec = spec;
    this.conf = conf;
    this.locationFactory = locationFactory;
  }

  @Override
  public void create() throws IOException {
    final byte[] name = Bytes.toBytes(HBaseTableUtil.getHBaseTableName(tableName));

    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(DATA_COLUMN_FAMILY);
    // todo: make stuff configurable
    // NOTE: we cannot limit number of versions as there's no hard limit on # of excluded from read txs
    //       also: currently we want max versions to support readless increments
    columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);

    String ttlProp = spec.getProperties().get(Table.PROPERTY_TTL);
    if (ttlProp != null) {
      long ttl = Long.parseLong(ttlProp);
      if (ttl > 0) {
        columnDescriptor.setValue(TxConstants.PROPERTY_TTL, String.valueOf(ttl));
      }
    }


    final HTableDescriptor tableDescriptor = new HTableDescriptor(name);
    tableDescriptor.addFamily(columnDescriptor);

    // if the dataset is configured for readless increments, the set the table property to support upgrades
    boolean supportsReadlessIncrements = supportsReadlessIncrements(spec);
    if (supportsReadlessIncrements) {
      tableDescriptor.setValue(Table.PROPERTY_READLESS_INCREMENT, "true");
    }

    // if the dataset is configured to be non-transactional, the set the table property to support upgrades
    if (!isTansactional(spec)) {
      tableDescriptor.setValue(Constants.Dataset.TABLE_TX_DISABLED, "true");
      if (supportsReadlessIncrements) {
        // readless increments CPs by default assume that table is transactional
        columnDescriptor.setValue("dataset.table.readless.increment.transactional", "false");
      }
    }

    CoprocessorJar coprocessorJar = createCoprocessorJar();

    for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
      addCoprocessor(tableDescriptor, coprocessor, coprocessorJar.getJarLocation(),
                     coprocessorJar.getPriority(coprocessor));
    }

    byte[][] splits = null;
    String splitsProperty = spec.getProperty(PROPERTY_SPLITS);
    if (splitsProperty != null) {
      splits = GSON.fromJson(splitsProperty, byte[][].class);
    }

    tableUtil.createTableIfNotExists(getAdmin(), name, tableDescriptor, splits);
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
    if (spec.getProperty(Table.PROPERTY_TTL) == null &&
        columnDescriptor.getValue(TxConstants.PROPERTY_TTL) != null) {
      columnDescriptor.remove(TxConstants.PROPERTY_TTL.getBytes());
      needUpgrade = true;
    } else if (spec.getProperty(Table.PROPERTY_TTL) != null &&
               !spec.getProperty(Table.PROPERTY_TTL).equals
                  (columnDescriptor.getValue(TxConstants.PROPERTY_TTL))) {
      columnDescriptor.setValue(TxConstants.PROPERTY_TTL, spec.getProperty(TxConstants.PROPERTY_TTL));
      needUpgrade = true;
    }

    // NOTE: transactional attribute for table cannot be changed between upgrades, currently

    // check if the readless increment setting has changed
    if (spec.getProperty(Table.PROPERTY_READLESS_INCREMENT) == null &&
        tableDescriptor.getValue(Table.PROPERTY_READLESS_INCREMENT) != null) {
      tableDescriptor.remove(Table.PROPERTY_READLESS_INCREMENT);
      needUpgrade = true;
    } else if (spec.getProperty(Table.PROPERTY_READLESS_INCREMENT) != null &&
        !spec.getProperty(Table.PROPERTY_READLESS_INCREMENT).equals(
            tableDescriptor.getValue(Table.PROPERTY_READLESS_INCREMENT))) {
      tableDescriptor.setValue(Table.PROPERTY_READLESS_INCREMENT,
          spec.getProperty(Table.PROPERTY_READLESS_INCREMENT));
      needUpgrade = true;
    }

    return needUpgrade;
  }

  @Override
  protected CoprocessorJar createCoprocessorJar() throws IOException {
    boolean supportsIncrement = supportsReadlessIncrements(spec);
    boolean transactional = isTansactional(spec);
    return createCoprocessorJarInternal(conf, locationFactory, tableUtil, transactional, supportsIncrement);
  }

  public static CoprocessorJar createCoprocessorJarInternal(CConfiguration conf,
                                                            LocationFactory locationFactory,
                                                            HBaseTableUtil tableUtil,
                                                            boolean transactional,
                                                            boolean supportsReadlessIncrement) throws IOException {
    // create the jar for the data janitor coprocessor.
    Location jarDir = locationFactory.create(conf.get(Constants.CFG_HDFS_LIB_DIR));
    Class<? extends Coprocessor> dataJanitorClass = tableUtil.getTransactionDataJanitorClassForVersion();
    Class<? extends Coprocessor> incrementClass = tableUtil.getIncrementHandlerClassForVersion();
    ImmutableList.Builder<Class<? extends Coprocessor>> coprocessors = ImmutableList.builder();
    if (transactional) {
      // tx janitor
      if (conf.getBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE,
                           TxConstants.DataJanitor.DEFAULT_TX_JANITOR_ENABLE)) {
        coprocessors.add(dataJanitorClass);
      }
    }
    // readless increments
    if (supportsReadlessIncrement) {
      coprocessors.add(incrementClass);
    }

    ImmutableList<Class<? extends Coprocessor>> coprocessorList = coprocessors.build();
    if (coprocessorList.isEmpty()) {
      return CoprocessorJar.EMPTY;
    }
    Location jarFile = HBaseTableUtil.createCoProcessorJar("table", jarDir, coprocessorList);
    return new CoprocessorJar(coprocessorList, jarFile);
  }

  /**
   * Returns whether or not the dataset defined in the given specification should enable read-less increments.
   * Defaults to false.
   */
  public static boolean supportsReadlessIncrements(DatasetSpecification spec) {
    return "true".equalsIgnoreCase(spec.getProperty(Table.PROPERTY_READLESS_INCREMENT));
  }

  /**
   * Returns whether or not the dataset defined in the given specification is transactional.
   * Defaults to true.
   */
  public static boolean isTansactional(DatasetSpecification spec) {
    return !"true".equalsIgnoreCase(spec.getProperty(Constants.Dataset.TABLE_TX_DISABLED));
  }
}
