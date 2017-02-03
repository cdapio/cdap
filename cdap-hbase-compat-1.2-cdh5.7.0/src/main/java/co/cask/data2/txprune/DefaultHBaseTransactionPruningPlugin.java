/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.data2.txprune;

import co.cask.cdap.data2.transaction.coprocessor.hbase12cdh570.DefaultTransactionProcessor;
import co.cask.cdap.data2.transaction.messaging.coprocessor.hbase12cdh570.MessageTableRegionObserver;
import co.cask.cdap.data2.transaction.queue.coprocessor.hbase12cdh570.HBaseQueueRegionObserver;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.tephra.hbase.txprune.DataJanitorState;
import org.apache.tephra.hbase.txprune.HBaseTransactionPruningPlugin;
import org.apache.tephra.txprune.TransactionPruningPlugin;

import java.io.IOException;

/**
 * {@link TransactionPruningPlugin} for CDAP Datasets, TMS Message Table, HBase Queues.
 */
public class DefaultHBaseTransactionPruningPlugin extends HBaseTransactionPruningPlugin {

  // We need to copy this method from the parent class since the HTableDescriptor#addFamily method returns void
  // in HBase 1.2-CDH5.7.0 while it returns HTableDescriptor HBase 1.1 which is what the tephra-hbase-compat-1.1 module
  // depends on.
  @Override
  protected void createPruneTable(TableName stateTable) throws IOException {
    try (Admin admin = this.connection.getAdmin()) {
      if (admin.tableExists(stateTable)) {
        LOG.debug("Not creating pruneStateTable {} since it already exists.",
                  stateTable.getNameWithNamespaceInclAsString());
        return;
      }

      HTableDescriptor htd = new HTableDescriptor(stateTable);
      htd.addFamily(new HColumnDescriptor(DataJanitorState.FAMILY).setMaxVersions(1));
      admin.createTable(htd);
      LOG.info("Created pruneTable {}", stateTable.getNameWithNamespaceInclAsString());
    } catch (TableExistsException ex) {
      // Expected if the prune state table is being created at the same time by another client
      LOG.debug("Not creating pruneStateTable {} since it already exists.",
                stateTable.getNameWithNamespaceInclAsString(), ex);
    }
  }

  @Override
  protected boolean isTransactionalTable(HTableDescriptor tableDescriptor) {
    return tableDescriptor.hasCoprocessor(DefaultTransactionProcessor.class.getName()) ||
      tableDescriptor.hasCoprocessor(MessageTableRegionObserver.class.getName()) ||
      tableDescriptor.hasCoprocessor(HBaseQueueRegionObserver.class.getName());
  }
}
