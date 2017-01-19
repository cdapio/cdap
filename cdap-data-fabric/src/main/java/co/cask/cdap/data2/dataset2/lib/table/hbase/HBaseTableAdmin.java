/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.tephra.TxConstants;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
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
          hConf, conf, tableUtil);
    this.spec = spec;
    this.conf = conf;
    this.locationFactory = locationFactory;
  }

  @Override
  public void create() throws IOException {
    HColumnDescriptor columnDescriptor =
      new HColumnDescriptor(TableProperties.getColumnFamilyBytes(spec.getProperties()));

    if (TableProperties.getReadlessIncrementSupport(spec.getProperties())) {
      columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    } else if (DatasetsUtil.isTransactional(spec.getProperties())) {
      // NOTE: we cannot limit number of versions as there's no hard limit on # of excluded from read txs
      columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    } else {
      columnDescriptor.setMaxVersions(1);
    }

    tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);

    Long ttl = TableProperties.getTTL(spec.getProperties());
    if (ttl != null) {
      // convert ttl from seconds to milli-seconds
      ttl = TimeUnit.SECONDS.toMillis(ttl);
      columnDescriptor.setValue(TxConstants.PROPERTY_TTL, String.valueOf(ttl));
    }

    final HTableDescriptorBuilder tableDescriptor = tableUtil.buildHTableDescriptor(tableId);
    tableDescriptor.addFamily(columnDescriptor);

    // if the dataset is configured for read-less increments, then set the table property to support upgrades
    boolean supportsReadlessIncrements = TableProperties.getReadlessIncrementSupport(spec.getProperties());
    if (supportsReadlessIncrements) {
      tableDescriptor.setValue(Table.PROPERTY_READLESS_INCREMENT, "true");
    }

    // if the dataset is configured to be non-transactional, then set the table property to support upgrades
    if (!DatasetsUtil.isTransactional(spec.getProperties())) {
      tableDescriptor.setValue(Constants.Dataset.TABLE_TX_DISABLED, "true");
      if (supportsReadlessIncrements) {
        // read-less increments CPs by default assume that table is transactional
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

    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      tableUtil.createTableIfNotExists(admin, tableId, tableDescriptor.build(), splits);
    }
  }

  @Override
  public void update(DatasetSpecification oldSpec) throws IOException {
    updateTable(false);
  }

  @Override
  protected boolean needsUpdate(HTableDescriptor tableDescriptor) {
    HColumnDescriptor columnDescriptor =
      tableDescriptor.getFamily(TableProperties.getColumnFamilyBytes(spec.getProperties()));

    boolean needUpgrade = false;
    if (tableUtil.getBloomFilter(columnDescriptor) != HBaseTableUtil.BloomType.ROW) {
      tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);
      needUpgrade = true;
    }
    Long ttl = TableProperties.getTTL(spec.getProperties());
    String ttlInMillis = ttl == null ? null : String.valueOf(TimeUnit.SECONDS.toMillis(ttl));

    if (ttl == null && columnDescriptor.getValue(TxConstants.PROPERTY_TTL) != null) {
      columnDescriptor.remove(TxConstants.PROPERTY_TTL.getBytes());
      needUpgrade = true;
    } else if (ttl != null && !ttlInMillis.equals(columnDescriptor.getValue(TxConstants.PROPERTY_TTL))) {
      columnDescriptor.setValue(TxConstants.PROPERTY_TTL, ttlInMillis);
      needUpgrade = true;
    }

    // NOTE: transactional attribute for table cannot be changed between upgrades, currently

    // check if the read-less increment setting has changed
    boolean supportsReadlessIncrements = supportsReadlessIncrements(tableDescriptor);
    boolean specifiedReadlessIncrements = TableProperties.getReadlessIncrementSupport(spec.getProperties());
    if (!specifiedReadlessIncrements && supportsReadlessIncrements) {
      tableDescriptor.remove(Table.PROPERTY_READLESS_INCREMENT);
      supportsReadlessIncrements = false;
      needUpgrade = true;
    } else if (specifiedReadlessIncrements && !supportsReadlessIncrements) {
      tableDescriptor.setValue(Table.PROPERTY_READLESS_INCREMENT, "true");
      supportsReadlessIncrements = true;
      needUpgrade = true;
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
    boolean supportsIncrement = TableProperties.getReadlessIncrementSupport(spec.getProperties());
    boolean transactional = DatasetsUtil.isTransactional(spec.getProperties());
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
      if (conf.getBoolean(Constants.Transaction.DataJanitor.CFG_TX_JANITOR_ENABLE,
                          Constants.Transaction.DataJanitor.DEFAULT_TX_JANITOR_ENABLE)) {
        coprocessors.add(dataJanitorClass);
      }
    }
    // read-less increments
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
