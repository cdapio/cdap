/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.data2.util.hbase.HTableNameConverterFactory;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Handles upgrade for System and User Datasets
 */
public class DatasetUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetUpgrader.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final HBaseTableUtil hBaseTableUtil;
  private final DatasetFramework dsFramework;
  private final Pattern defaultNSUserTablePrefix;
  private final String datasetTablePrefix;

  @Inject
  @VisibleForTesting
  DatasetUpgrader(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
                  NamespacedLocationFactory namespacedLocationFactory,
                  HBaseTableUtil hBaseTableUtil, DatasetFramework dsFramework) {

    super(locationFactory, namespacedLocationFactory);
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.hBaseTableUtil = hBaseTableUtil;
    this.dsFramework = dsFramework;
    this.datasetTablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    this.defaultNSUserTablePrefix = Pattern.compile(String.format("^%s\\.user\\..*",
                                                                  datasetTablePrefix));
  }

  @Override
  public void upgrade() throws Exception {
    // Upgrade system dataset
    upgradeSystemDatasets();

    // Upgrade all user hbase tables
    upgradeUserTables();
  }

  private void upgradeSystemDatasets() throws Exception {
    for (DatasetSpecificationSummary spec : dsFramework.getInstances(Constants.SYSTEM_NAMESPACE_ID)) {
      LOG.info("Upgrading dataset in system namespace: {}, spec: {}", spec.getName(), spec.toString());
      DatasetAdmin admin = dsFramework.getAdmin(Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE_ID, spec.getName()),
                                                null);
      // we know admin is not null, since we are looping over existing datasets
      //noinspection ConstantConditions
      admin.upgrade();
      LOG.info("Upgraded dataset: {}", spec.getName());
    }
  }

  private void upgradeUserTables() throws Exception {
    HBaseAdmin hAdmin = new HBaseAdmin(hConf);
    for (HTableDescriptor desc : hAdmin.listTables()) {
      if (isCDAPUserTable(desc)) {
        upgradeUserTable(desc);
      }
    }
  }


  private void upgradeUserTable(HTableDescriptor desc) throws IOException {
    HTableNameConverter hTableNameConverter = new HTableNameConverterFactory().get();
    TableId tableId = hTableNameConverter.from(desc);
    LOG.info("Upgrading hbase table: {}, desc: {}", tableId, desc);

    final boolean supportsIncrement = HBaseTableAdmin.supportsReadlessIncrements(desc);
    final boolean transactional = HBaseTableAdmin.isTransactional(desc);
    DatasetAdmin admin = new AbstractHBaseDataSetAdmin(tableId, hConf, hBaseTableUtil) {
      @Override
      protected CoprocessorJar createCoprocessorJar() throws IOException {
        return HBaseTableAdmin.createCoprocessorJarInternal(cConf,
                                                            locationFactory,
                                                            hBaseTableUtil,
                                                            transactional,
                                                            supportsIncrement);
      }

      @Override
      protected boolean upgradeTable(HTableDescriptor tableDescriptor) {
        return false;
      }

      @Override
      public void create() throws IOException {
        // no-op
        throw new UnsupportedOperationException("This DatasetAdmin is only used for upgrade() operation");
      }
    };
    admin.upgrade();
    LOG.info("Upgraded hbase table: {}", tableId);
  }


  private boolean isCDAPUserTable(HTableDescriptor desc) {
    String tableName = desc.getNameAsString();
    // If table is in system namespace: (starts with <tablePrefix>_system
    // or if it is not created by CDAP it is not user table
    if (tableName.startsWith(String.format("%s_%s", this.datasetTablePrefix, Constants.SYSTEM_NAMESPACE)) ||
       (!isTableCreatedByCDAP(desc))) {
      return false;
    }
    // User tables are named differently in default vs non-default namespace
    // User table in default namespace starts with cdap.user
    // User table in Non-default namespace is a table that doesn't have
    //    system.queue or system.stream or system.sharded.queue
    return defaultNSUserTablePrefix.matcher(tableName).matches() ||
      // Note: if the user has created a dataset called system.* then we will not upgrade the table.
      // CDAP-2977 should be fixed to have a cleaner fix for this.
      !(tableName.contains("system.queue") || tableName.contains("system.stream") ||
        tableName.contains("system.sharded.queue"));
  }

  // Note: This check can be safely used for user table since we create meta.
  // CDAP-2963 should be fixed so that we can make use of this check generically for all cdap tables
  private boolean isTableCreatedByCDAP(HTableDescriptor desc) {
    return (desc.getValue("cdap.version") != null);
  }
}
