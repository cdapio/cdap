/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.stream.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.hbase.ddl.ColumnFamilyDescriptor;
import co.cask.cdap.hbase.ddl.TableDescriptor;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Factory for creating {@link StreamConsumerStateStore} in HBase.
 */
public final class HBaseStreamConsumerStateStoreFactory implements StreamConsumerStateStoreFactory {
  private final Configuration hConf;
  private final CConfiguration cConf;
  private final HBaseTableUtil tableUtil;

  @Inject
  HBaseStreamConsumerStateStoreFactory(Configuration hConf, CConfiguration cConf, HBaseTableUtil tableUtil) {
    this.hConf = hConf;
    this.cConf = cConf;
    this.tableUtil = tableUtil;
  }

  @Override
  public synchronized StreamConsumerStateStore create(StreamConfig streamConfig) throws IOException {
    NamespaceId namespace = streamConfig.getStreamId().getParent();
    TableId streamStateStoreTableId = StreamUtils.getStateStoreTableId(namespace);
    TableId hbaseTableId = tableUtil.createHTableId(new NamespaceId(streamStateStoreTableId.getNamespace()),
                                                    streamStateStoreTableId.getTableName());
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      if (!tableUtil.tableExists(admin, hbaseTableId)) {

        TableDescriptor.Builder tbdBuilder = new TableDescriptor.Builder(cConf, hbaseTableId);
        ColumnFamilyDescriptor.Builder cfdBuilder
          = new ColumnFamilyDescriptor.Builder(hConf, Bytes.toString(QueueEntryRow.COLUMN_FAMILY)).setMaxVersions(1);

        tbdBuilder.addColumnFamily(cfdBuilder.build());
        tableUtil.createTableIfNotExists(admin, hbaseTableId, tbdBuilder.build(), null,
                                         QueueConstants.MAX_CREATE_TABLE_WAIT, TimeUnit.MILLISECONDS);
      }
    }

    HTable hTable = tableUtil.createHTable(hConf, hbaseTableId);
    hTable.setWriteBufferSize(Constants.Stream.HBASE_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    return new HBaseStreamConsumerStateStore(streamConfig, hTable);
  }

  @Override
  public synchronized void dropAllInNamespace(NamespaceId namespace) throws IOException {
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      TableId tableId = StreamUtils.getStateStoreTableId(namespace);
      TableId hbaseTableId = tableUtil.createHTableId(new NamespaceId(tableId.getNamespace()), tableId.getTableName());
      if (tableUtil.tableExists(admin, hbaseTableId)) {
        tableUtil.dropTable(admin, hbaseTableId);
      }
    }
  }
}
