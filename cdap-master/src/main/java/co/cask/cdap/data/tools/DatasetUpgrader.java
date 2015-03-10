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
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceMDSUpgrader;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetTypeMDSUpgrader;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.data2.util.hbase.HTableNameConverterFactory;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import com.google.inject.name.Named;
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
  private final QueueAdmin queueAdmin;
  private final HBaseTableUtil hBaseTableUtil;
  private final DatasetFramework dsFramework;
  private static final Pattern USER_TABLE_PREFIX = Pattern.compile("^cdap\\.user\\..*");
  private final DatasetInstanceMDSUpgrader datasetInstanceMDSUpgrader;
  private final DatasetTypeMDSUpgrader datasetTypeMDSUpgrader;

  @Inject
  private DatasetUpgrader(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
                          QueueAdmin queueAdmin, HBaseTableUtil hBaseTableUtil,
                          @Named("dsFramework") final DatasetFramework dsFramework,
                          DatasetInstanceMDSUpgrader datasetInstanceMDSUpgrader,
                          DatasetTypeMDSUpgrader datasetTypeMDSUpgrader) {

    super(locationFactory);
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.queueAdmin = queueAdmin;
    this.hBaseTableUtil = hBaseTableUtil;
    this.dsFramework = dsFramework;
    this.datasetInstanceMDSUpgrader = datasetInstanceMDSUpgrader;
    this.datasetTypeMDSUpgrader = datasetTypeMDSUpgrader;


  }

  @Override
  public void upgrade() throws Exception {
    // Upgrade system dataset
    upgradeSystemDatasets();

    // Upgrade all user hbase tables
    upgradeUserTables();

    // Upgrade all queue and stream tables.
    queueAdmin.upgrade();

    // Upgrade the datasets meta meta table
    datasetTypeMDSUpgrader.upgrade();

    // Upgrade the datasets instance meta table
    datasetInstanceMDSUpgrader.upgrade();
  }

  private void upgradeSystemDatasets() throws Exception {

    // Upgrade all datasets in system namespace
    for (DatasetSpecificationSummary spec : dsFramework.getInstances(Constants.DEFAULT_NAMESPACE_ID)) {
      LOG.info("Upgrading dataset: {}, spec: {}", spec.getName(), spec.toString());
      DatasetAdmin admin = dsFramework.getAdmin(Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, spec.getName()),
                                                null);
      // we know admin is not null, since we are looping over existing datasets
      admin.upgrade();
      LOG.info("Upgraded dataset: {}", spec.getName());
    }
  }

  private void upgradeUserTables() throws Exception {
    HBaseAdmin hAdmin = new HBaseAdmin(hConf);

    for (HTableDescriptor desc : hAdmin.listTables(USER_TABLE_PREFIX)) {
      String tableName = desc.getNameAsString();
      HTableNameConverter hTableNameConverter = new HTableNameConverterFactory().get();
      TableId tableId = hTableNameConverter.from(tableName);
      LOG.info("Upgrading hbase table: {}, desc: {}", tableName, desc);

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
          // we don't do any other changes apart from coprocessors upgrade
          return false;
        }

        @Override
        public void create() throws IOException {
          // no-op
          throw new UnsupportedOperationException("This DatasetAdmin is only used for upgrade() operation");
        }
      };
      admin.upgrade();
      LOG.info("Upgraded hbase table: {}", tableName);
    }
  }

}
