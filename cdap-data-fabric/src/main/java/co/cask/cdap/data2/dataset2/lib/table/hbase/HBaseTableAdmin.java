/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.Updatable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.TableProperties;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.proto.id.NamespaceId;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HBaseTableAdmin extends AbstractHBaseDataSetAdmin implements Updatable {
  public static final String PROPERTY_SPLITS = "hbase.splits";

  private static final Gson GSON = new Gson();

  private final DatasetSpecification spec;
  // todo: datasets should not depend on cdap configuration!
  private final CConfiguration conf;

  private final LocationFactory locationFactory;

  public HBaseTableAdmin(DatasetContext datasetContext,
                         DatasetSpecification spec,
                         Configuration hConf,
                         HBaseTableUtil tableUtil,
                         CConfiguration conf,
                         LocationFactory locationFactory) throws IOException {
    super(tableUtil.createHTableId(new NamespaceId(datasetContext.getNamespaceId()), spec.getName()),
          hConf, tableUtil);
    this.spec = spec;
    this.conf = conf;
    this.locationFactory = locationFactory;
  }

  @Override
  public void create() throws IOException {
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(TableProperties.getColumnFamily(spec.getProperties()));

    if (TableProperties.supportsReadlessIncrements(spec.getProperties())) {
      columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    } else if (TableProperties.isTransactional(spec.getProperties())) {
      // NOTE: we cannot limit number of versions as there's no hard limit on # of excluded from read txs
      columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    } else {
      columnDescriptor.setMaxVersions(1);
    }

    tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);

    String ttlProp = spec.getProperties().get(Table.PROPERTY_TTL);
    if (ttlProp != null) {
      long ttl = Long.parseLong(ttlProp);
      if (ttl > 0) {
        // convert ttl from seconds to milli-seconds
        ttl = TimeUnit.SECONDS.toMillis(ttl);
        columnDescriptor.setValue(TxConstants.PROPERTY_TTL, String.valueOf(ttl));
      }
    }

    final HTableDescriptorBuilder tableDescriptor = tableUtil.buildHTableDescriptor(tableId);
    tableDescriptor.addFamily(columnDescriptor);

    // if the dataset is configured for readless increments, the set the table property to support upgrades
    boolean supportsReadlessIncrements = TableProperties.supportsReadlessIncrements(spec.getProperties());
    if (supportsReadlessIncrements) {
      tableDescriptor.setValue(Table.PROPERTY_READLESS_INCREMENT, "true");
    }

    // if the dataset is configured to be non-transactional, the set the table property to support upgrades
    if (!TableProperties.isTransactional(spec.getProperties())) {
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

    tableUtil.createTableIfNotExists(getAdmin(), tableId, tableDescriptor.build(), splits);
  }

  @Override
  public void update(DatasetSpecification oldSpec) throws IOException {
    updateTable(false);
  }

  @Override
  protected boolean needsUpdate(HTableDescriptor tableDescriptor) {
    HColumnDescriptor columnDescriptor =
      tableDescriptor.getFamily(TableProperties.getColumnFamily(spec.getProperties()));

    boolean needUpgrade = false;
    if (tableUtil.getBloomFilter(columnDescriptor) != HBaseTableUtil.BloomType.ROW) {
      tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);
      needUpgrade = true;
    }
    String ttlInMillis = null;
    if (spec.getProperty(Table.PROPERTY_TTL) != null) {
      // ttl not null, convert to millis
      ttlInMillis = String.valueOf(TimeUnit.SECONDS.toMillis(Long.valueOf(spec.getProperty(Table.PROPERTY_TTL))));
    }

    if (spec.getProperty(Table.PROPERTY_TTL) == null &&
        columnDescriptor.getValue(TxConstants.PROPERTY_TTL) != null) {
      columnDescriptor.remove(TxConstants.PROPERTY_TTL.getBytes());
      needUpgrade = true;
    } else if (spec.getProperty(Table.PROPERTY_TTL) != null &&
               !ttlInMillis.equals(columnDescriptor.getValue(TxConstants.PROPERTY_TTL))) {
      columnDescriptor.setValue(TxConstants.PROPERTY_TTL, ttlInMillis);
      needUpgrade = true;
    }

    // NOTE: transactional attribute for table cannot be changed between upgrades, currently

    // check if the readless increment setting has changed
    boolean supportsReadlessIncrements;
    if (spec.getProperty(Table.PROPERTY_READLESS_INCREMENT) == null &&
        tableDescriptor.getValue(Table.PROPERTY_READLESS_INCREMENT) != null) {
      tableDescriptor.remove(Table.PROPERTY_READLESS_INCREMENT);
      supportsReadlessIncrements = false;
      needUpgrade = true;
    } else if (spec.getProperty(Table.PROPERTY_READLESS_INCREMENT) != null &&
        !spec.getProperty(Table.PROPERTY_READLESS_INCREMENT).equals(
            tableDescriptor.getValue(Table.PROPERTY_READLESS_INCREMENT))) {
      tableDescriptor.setValue(Table.PROPERTY_READLESS_INCREMENT,
          spec.getProperty(Table.PROPERTY_READLESS_INCREMENT));
      supportsReadlessIncrements = true;
      needUpgrade = true;
    } else {
      supportsReadlessIncrements = supportsReadlessIncrements(tableDescriptor);
    }

    boolean setMaxVersions = supportsReadlessIncrements || HBaseTableAdmin.isTransactional(tableDescriptor);
    if (setMaxVersions && columnDescriptor.getMaxVersions() < Integer.MAX_VALUE) {
      columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
      needUpgrade = true;
    }

    return needUpgrade;
  }

  @Override
  protected CoprocessorJar createCoprocessorJar() throws IOException {
    boolean supportsIncrement = TableProperties.supportsReadlessIncrements(spec.getProperties());
    boolean transactional = TableProperties.isTransactional(spec.getProperties());
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
   *
   * @deprecated use {@link TableProperties#supportsReadlessIncrements(Map)} instead
   */
  @Deprecated
  @SuppressWarnings("unused")
  public static boolean supportsReadlessIncrements(Map<String, String> props) {
    return TableProperties.supportsReadlessIncrements(props);
  }

  /**
   * Returns whether or not the dataset defined in the given specification is transactional.
   * Defaults to true.
   *
   * @deprecated use {@link TableProperties#isTransactional(Map)} instead
   */
  @Deprecated
  @SuppressWarnings("unused")
  public static boolean isTransactional(Map<String, String> props) {
    return TableProperties.isTransactional(props);
  }

  /**
   * Returns the column family as being set in the given specification.
   * If it is not set, the {@link TableProperties#DEFAULT_DATA_COLUMN_FAMILY} will be returned.
   *
   * @deprecated use {@link TableProperties#getColumnFamily(Map)} instead
   */
  @Deprecated
  @SuppressWarnings("unused")
  public static byte[] getColumnFamily(Map<String, String> props) {
    return TableProperties.getColumnFamily(props);
  }

  /**
   * Returns whether or not the table defined by the given descriptor has read-less increments enabled.
   * Defaults to false.
   */
  public static boolean supportsReadlessIncrements(HTableDescriptor desc) {
    return "true".equalsIgnoreCase(desc.getValue(Table.PROPERTY_READLESS_INCREMENT));
  }

  /**
   * Returns whether or not the table defined by the given descriptor has transactions enabled.
   * Defaults to true.
   */
  public static boolean isTransactional(HTableDescriptor desc) {
    return !"true".equalsIgnoreCase(desc.getValue(Constants.Dataset.TABLE_TX_DISABLED));
  }
}
