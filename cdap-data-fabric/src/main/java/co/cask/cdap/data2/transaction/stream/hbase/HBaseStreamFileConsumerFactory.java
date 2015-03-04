/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.data.file.FileReader;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.data.stream.StreamEventOffset;
import co.cask.cdap.data.stream.StreamFileOffset;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.transaction.stream.AbstractStreamFileConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumerState;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.TableId;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A {@link co.cask.cdap.data2.transaction.stream.StreamConsumerFactory} that reads from stream file
 * and uses HBase as the store for consumer process states.
 */
public final class HBaseStreamFileConsumerFactory extends AbstractStreamFileConsumerFactory {

  private final HBaseTableUtil tableUtil;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private HBaseAdmin admin;

  @Inject
  HBaseStreamFileConsumerFactory(StreamAdmin streamAdmin,
                                 StreamConsumerStateStoreFactory stateStoreFactory,
                                 CConfiguration cConf, Configuration hConf, HBaseTableUtil tableUtil) {
    super(cConf, streamAdmin, stateStoreFactory);
    this.hConf = hConf;
    this.cConf = cConf;
    this.tableUtil = tableUtil;
  }

  @Override
  protected StreamConsumer create(String tableName, StreamConfig streamConfig, ConsumerConfig consumerConfig,
                                  StreamConsumerStateStore stateStore, StreamConsumerState beginConsumerState,
                                  FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                                  @Nullable ReadFilter extraFilter) throws IOException {

    String hBaseTableName = HBaseTableUtil.getHBaseTableName(tableName);
    TableId tableId = TableId.from(hBaseTableName);
    HTableDescriptor htd = tableUtil.createHTableDescriptor(tableId);

    HColumnDescriptor hcd = new HColumnDescriptor(QueueEntryRow.COLUMN_FAMILY);
    htd.addFamily(hcd);
    hcd.setMaxVersions(1);

    int splits = cConf.getInt(Constants.Stream.CONSUMER_TABLE_PRESPLITS, 1);
    byte[][] splitKeys = HBaseTableUtil.getSplitKeys(splits);

    tableUtil.createTableIfNotExists(getAdmin(), tableId, htd, splitKeys,
                                     QueueConstants.MAX_CREATE_TABLE_WAIT, TimeUnit.MILLISECONDS);

    HTable hTable = tableUtil.createHTable(hConf, tableId);
    hTable.setWriteBufferSize(Constants.Stream.HBASE_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    return new HBaseStreamFileConsumer(cConf, streamConfig, consumerConfig, hTable, reader,
                                       stateStore, beginConsumerState, extraFilter,
                                       HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR);
  }

  @Override
  protected void dropTable(String tableName) throws IOException {
    HBaseAdmin admin = getAdmin();
    TableId tableId = TableId.from(tableName);
    if (tableUtil.tableExists(admin, tableId)) {
      tableUtil.disableTable(admin, tableId);
      tableUtil.deleteTable(admin, tableId);
    }
  }

  private synchronized HBaseAdmin getAdmin() throws IOException {
    if (admin == null) {
      admin = new HBaseAdmin(hConf);
    }
    return admin;
  }
}
