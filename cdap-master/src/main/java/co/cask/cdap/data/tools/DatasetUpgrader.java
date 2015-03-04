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
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.TableId;
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
  private final DatasetFramework namespacedFramework;

  @Inject
  private DatasetUpgrader(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
                          QueueAdmin queueAdmin, HBaseTableUtil hBaseTableUtil,
                          @Named("namespacedDSFramework") DatasetFramework namespacedFramework) {

    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.queueAdmin = queueAdmin;
    this.hBaseTableUtil = hBaseTableUtil;
    this.namespacedFramework = namespacedFramework;
  }

  @Override
  public void upgrade() throws Exception {
    // Upgrade system dataset
    upgradeSystemDatasets(namespacedFramework);

    // Upgrade all user hbase tables
    upgradeUserTables();

    // Upgrade all queue and stream tables.
    queueAdmin.upgrade();
  }

  private void upgradeSystemDatasets(DatasetFramework framework) throws Exception {

    // Upgrade all datasets in system namespace
    Id.Namespace systemNamespace = Constants.SYSTEM_NAMESPACE_ID;
    for (DatasetSpecification spec : framework.getInstances(systemNamespace)) {
      LOG.info("Upgrading dataset: {}, spec: {}", spec.getName(), spec.toString());
      DatasetAdmin admin = framework.getAdmin(Id.DatasetInstance.from(systemNamespace, spec.getName()), null);
      // we know admin is not null, since we are looping over existing datasets
      admin.upgrade();
      LOG.info("Upgraded dataset: {}", spec.getName());
    }
  }

  private void upgradeUserTables() throws Exception {
    DefaultDatasetNamespace namespace = new DefaultDatasetNamespace(cConf);
    HBaseAdmin hAdmin = new HBaseAdmin(hConf);


    for (HTableDescriptor desc : hAdmin.listTables()) {
      String tableName = desc.getNameAsString();
      TableId tableId = TableId.from(tableName);
      Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(tableId.getNamespace(), tableId.getTableName());
      // todo: it works now, but we will want to change it if namespacing of datasets in HBase is more than +prefix
      if (namespace.fromNamespaced(datasetInstanceId) != null) {
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
}
