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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.Id;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * Upgrades queue.config table
 */
public class QueueConfigUpgrader extends AbstractUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(QueueConfigUpgrader.class);
  private final HBaseTableUtil tableUtil;

  @Inject
  public QueueConfigUpgrader(LocationFactory locationFactory, HBaseTableUtil tableUtil) {
    super(locationFactory);
    this.tableUtil = tableUtil;
  }

  @Override
  void upgrade() throws Exception {
    String dsName = Joiner.on(".").join(Constants.SYSTEM_NAMESPACE, QueueConstants.QUEUE_CONFIG_TABLE_NAME);
    Id.DatasetInstance datasetId = Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, dsName);
    HTable queueConfigTable = tableUtil.createHTable(HBaseConfiguration.create(),
                                                     TableId.from(datasetId.getNamespaceId(), dsName));
    LOG.info("Starting upgrade for queue config table {}", Bytes.toString(queueConfigTable.getTableName()));
    try {
      Scan scan = new Scan();
      scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
      scan.addFamily(QueueEntryRow.COLUMN_FAMILY);
      scan.setMaxVersions(1); // we only need to see one version of each row
      List<Mutation> mutations = Lists.newArrayList();
      Result result;
      ResultScanner resultScanner = queueConfigTable.getScanner(scan);
      try {
        while ((result = resultScanner.next()) != null) {
          byte[] row = result.getRow();
          String rowKey = Bytes.toStringBinary(row);
          LOG.debug("Processing queue config for  {}", rowKey);
          NavigableMap<byte[], byte[]> columnsMap = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);
          QueueName queueName = fromRowKey(row);
          if (queueName != null) {
            Put put = new Put(queueName.toBytes());
            LOG.debug("Upgrading queue name: {}", queueName);
            for (NavigableMap.Entry<byte[], byte[]> entry : columnsMap.entrySet()) {
              LOG.debug("Adding entry {} -> {} for upgrade",
                        Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
              put.add(QueueEntryRow.COLUMN_FAMILY, entry.getKey(), entry.getValue());
              mutations.add(put);
            }
            LOG.debug("Marking old key {} for deletion", rowKey);
            mutations.add(new Delete(row));
          }
          LOG.info("Finished processing queue config for {}", rowKey);
        }
      } finally {
        resultScanner.close();
      }

      queueConfigTable.batch(mutations);

      LOG.info("Successfully completed upgrade for queue config table {}",
               Bytes.toString(queueConfigTable.getTableName()));
    } catch (Exception e) {
      LOG.error("Error while upgrading queue config table: {}", datasetId, e);
      throw Throwables.propagate(e);
    } finally {
      queueConfigTable.close();
    }
  }

  @Nullable
  private QueueName fromRowKey(byte[] oldRowKey) {
    QueueName queueName = QueueName.from(oldRowKey);

    if (queueName.getNumComponents() == 4) {
      LOG.debug("Found old queue config {}. Upgrading.", queueName);
      return QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE,
                                   queueName.getFirstComponent(),
                                   queueName.getSecondComponent(),
                                   queueName.getThirdComponent(),
                                   queueName.getFourthComponent());
    } else if (queueName.getNumComponents() == 5) {
      LOG.debug("Queue config for {} seems to already have been upgraded. Skipping.", queueName);
    } else {
      LOG.warn("Unknown format for queue config {}. Skipping.", queueName);
    }
    // skip unknown rows or rows that may already have a namespace in the QueueName
    return null;
  }
}
