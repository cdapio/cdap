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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.file.FileReader;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.data.stream.StreamEventOffset;
import co.cask.cdap.data.stream.StreamFileOffset;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.stream.AbstractStreamFileConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumerState;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.ColumnFamilyDescriptorBuilder;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.TableDescriptorBuilder;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A {@link co.cask.cdap.data2.transaction.stream.StreamConsumerFactory} that reads from stream file
 * and uses HBase as the store for consumer process states.
 */
public final class HBaseStreamFileConsumerFactory extends AbstractStreamFileConsumerFactory {

  private final HBaseTableUtil tableUtil;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HBaseDDLExecutorFactory ddlExecutorFactory;

  @Inject
  HBaseStreamFileConsumerFactory(StreamAdmin streamAdmin, StreamConsumerStateStoreFactory stateStoreFactory,
                                 CConfiguration cConf, Configuration hConf, HBaseTableUtil tableUtil) {
    super(cConf, streamAdmin, stateStoreFactory);
    this.hConf = hConf;
    this.cConf = cConf;
    this.tableUtil = tableUtil;
    this.ddlExecutorFactory = new HBaseDDLExecutorFactory(cConf, hConf);
  }

  @Override
  protected StreamConsumer create(TableId tableId, StreamConfig streamConfig, ConsumerConfig consumerConfig,
                                  StreamConsumerStateStore stateStore, StreamConsumerState beginConsumerState,
                                  FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                                  @Nullable ReadFilter extraFilter) throws IOException {

    int splits = cConf.getInt(Constants.Stream.CONSUMER_TABLE_PRESPLITS);
    AbstractRowKeyDistributor distributor = new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(splits));

    byte[][] splitKeys = HBaseTableUtil.getSplitKeys(splits, splits, distributor);

    TableId hBaseTableId = tableUtil.createHTableId(new NamespaceId(tableId.getNamespace()), tableId.getTableName());

    TableDescriptorBuilder tdBuilder = HBaseTableUtil.getTableDescriptorBuilder(hBaseTableId, cConf);

    ColumnFamilyDescriptorBuilder cfdBuilder =
      HBaseTableUtil.getColumnFamilyDescriptorBuilder(Bytes.toString(QueueEntryRow.COLUMN_FAMILY), hConf);

    tdBuilder.addColumnFamily(cfdBuilder.build());

    tdBuilder.addProperty(QueueConstants.DISTRIBUTOR_BUCKETS, Integer.toString(splits));

    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
      ddlExecutor.createTableIfNotExists(tdBuilder.build(), splitKeys);
    }

    HTable hTable = tableUtil.createHTable(hConf, hBaseTableId);
    hTable.setWriteBufferSize(Constants.Stream.HBASE_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);

    return new HBaseStreamFileConsumer(cConf, streamConfig, consumerConfig, tableUtil, hTable, reader,
                                       stateStore, beginConsumerState, extraFilter,
                                       createKeyDistributor(hTable.getTableDescriptor()));
  }

  @Override
  protected void dropTable(TableId tableId) throws IOException {
    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get(); HBaseAdmin admin = new HBaseAdmin(hConf)) {
      TableId hBaseTableId = tableUtil.createHTableId(new NamespaceId(tableId.getNamespace()), tableId.getTableName());
      if (tableUtil.tableExists(admin, hBaseTableId)) {
        tableUtil.dropTable(ddlExecutor, hBaseTableId);
      }
    }
  }

  /**
   * Creates a {@link AbstractRowKeyDistributor} based on the meta data in the given {@link HTableDescriptor}.
   */
  private AbstractRowKeyDistributor createKeyDistributor(HTableDescriptor htd) {
    int buckets = QueueConstants.DEFAULT_ROW_KEY_BUCKETS;
    String value = htd.getValue(QueueConstants.DISTRIBUTOR_BUCKETS);

    if (value != null) {
      buckets = Integer.parseInt(value);
    }

    return new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(buckets));
  }
}
