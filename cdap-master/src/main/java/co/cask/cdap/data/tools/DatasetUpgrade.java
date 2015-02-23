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
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseOrderedTableAdmin;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.TableId;
import co.cask.cdap.proto.Id;
import com.google.inject.Injector;
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
public class DatasetUpgrade extends AbstractUpgrade implements Upgrade {

  private static final Logger LOG = LoggerFactory.getLogger(MDSUpgrade.class);

  @Override
  public void upgrade(Injector injector) throws Exception {
    // Upgrade system dataset
    upgradeSystemDatasets(injector, namespacedFramework);

    // Upgrade all user hbase tables
    upgradeUserTables(injector);

    // Upgrade all queue and stream tables.
    QueueAdmin queueAdmin = injector.getInstance(QueueAdmin.class);
    queueAdmin.upgrade();
  }

  private void upgradeSystemDatasets(Injector injector, DatasetFramework framework) throws Exception {

    // Upgrade all datasets in system namespace
    Id.Namespace systemNamespace = Id.Namespace.from(Constants.SYSTEM_NAMESPACE);
    for (DatasetSpecification spec : framework.getInstances(systemNamespace)) {
      System.out.println(String.format("Upgrading dataset: %s, spec: %s", spec.getName(), spec.toString()));
      DatasetAdmin admin = framework.getAdmin(Id.DatasetInstance.from(systemNamespace, spec.getName()), null);
      // we know admin is not null, since we are looping over existing datasets
      admin.upgrade();
      System.out.println(String.format("Upgraded dataset: %s", spec.getName()));
    }
  }

  private static void upgradeUserTables(final Injector injector) throws Exception {
    // We assume that all tables in USER namespace belong to OrderedTable type datasets. So we loop thru them
    // and upgrading with the help of HBaseOrderedTableAdmin
    DefaultDatasetNamespace namespace = new DefaultDatasetNamespace(cConf);

    Configuration hConf = injector.getInstance(Configuration.class);
    HBaseAdmin hAdmin = new HBaseAdmin(hConf);
    final HBaseTableUtil hBaseTableUtil = injector.getInstance(HBaseTableUtil.class);

    for (HTableDescriptor desc : hAdmin.listTables()) {
      String tableName = desc.getNameAsString();
      TableId tableId = TableId.from(tableName);
      Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(tableId.getNamespace(), tableId.getTableName());
      // todo: it works now, but we will want to change it if namespacing of datasets in HBase is more than +prefix
      if (namespace.fromNamespaced(datasetInstanceId) != null) {
        System.out.println(String.format("Upgrading hbase table: %s, desc: %s", tableName, desc.toString()));

        final boolean supportsIncrement =
          "true".equalsIgnoreCase(desc.getValue(OrderedTable.PROPERTY_READLESS_INCREMENT));
        DatasetAdmin admin = new AbstractHBaseDataSetAdmin(tableName, hConf, hBaseTableUtil) {
          @Override
          protected CoprocessorJar createCoprocessorJar() throws IOException {
            return HBaseOrderedTableAdmin.createCoprocessorJarInternal(cConf,
                                                                       injector.getInstance(LocationFactory.class),
                                                                       hBaseTableUtil,
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
        System.out.println(String.format("Upgraded hbase table: %s", tableName));
      }
    }
  }
}
