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
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
 * Upgrades stream state store table
 */
public class StreamStateStoreUpgrader extends AbstractUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(StreamStateStoreUpgrader.class);
  private final HBaseTableUtil tableUtil;
  private final Configuration conf;

  @Inject
  public StreamStateStoreUpgrader(LocationFactory locationFactory, HBaseTableUtil tableUtil) {
    super(locationFactory);
    this.tableUtil = tableUtil;
    this.conf = HBaseConfiguration.create();
  }

  @Override
  void upgrade() throws Exception {
    TableId streamStateStoreTableId = StreamUtils.getStateStoreTableId(Constants.DEFAULT_NAMESPACE_ID);
    if (!tableUtil.tableExists(new HBaseAdmin(conf), streamStateStoreTableId)) {
      LOG.info("Stream state store table does not exist: {}. No upgrade necessary.", streamStateStoreTableId);
      return;
    }
    HTable streamStateStoreHTable = tableUtil.createHTable(conf, streamStateStoreTableId);
    LOG.info("Starting upgrade for stream state store table: {}",
             Bytes.toString(streamStateStoreHTable.getTableName()));
    try {
      Scan scan = new Scan();
      scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
      scan.addFamily(QueueEntryRow.COLUMN_FAMILY);
      scan.setMaxVersions(1); // we only need to see one version of each row
      List<Mutation> mutations = Lists.newArrayList();
      Result result;
      ResultScanner resultScanner = streamStateStoreHTable.getScanner(scan);
      try {
        while ((result = resultScanner.next()) != null) {
          byte[] row = result.getRow();
          String rowKey = Bytes.toString(row);
          LOG.debug("Processing stream state for {}", rowKey);
          NavigableMap<byte[], byte[]> columnsMap = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);
          Id.Stream streamId = fromRowKey(row);
          if (streamId != null) {
            Put put = new Put(streamId.toBytes());
            LOG.debug("Upgrading stream: {}", streamId);
            for (NavigableMap.Entry<byte[], byte[]> entry : columnsMap.entrySet()) {
              LOG.debug("Adding entry {} -> {} for upgrade",
                        Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
              put.add(QueueEntryRow.COLUMN_FAMILY, entry.getKey(), entry.getValue());
              mutations.add(put);
            }
            LOG.debug("Marking old key {} for deletion", rowKey);
            mutations.add(new Delete(row));
          }
          LOG.info("Finished processing stream state for {}", rowKey);
        }
      } finally {
        resultScanner.close();
      }

      streamStateStoreHTable.batch(mutations);

      LOG.info("Successfully completed upgrade for stream state store table {}",
               Bytes.toString(streamStateStoreHTable.getTableName()));
    } catch (Exception e) {
      LOG.error("Error while upgrading stream state store table: {}", streamStateStoreHTable, e);
      throw Throwables.propagate(e);
    } finally {
      streamStateStoreHTable.close();
    }
  }

  @Nullable
  private Id.Stream fromRowKey(byte[] oldRowKey) {
    QueueName queueName = QueueName.from(oldRowKey);

    if (queueName.isStream() && queueName.getNumComponents() == 1) {
      LOG.debug("Found old stream state {}. Upgrading.", queueName);
      return Id.Stream.from(Constants.DEFAULT_NAMESPACE, queueName.getFirstComponent());
    } else {
      LOG.warn("Unknown format for queueName: {}. Skipping.", queueName);
    }

    // skip unknown rows or rows that may already have a namespace in the QueueName
    return null;
  }
}
