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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamProperties;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * admin for streams in hbase.
 */
@Singleton
public class HBaseStreamAdmin extends HBaseQueueAdmin implements StreamAdmin {

  private final TransactionExecutorFactory txExecutorFactory;

  @Inject
  public HBaseStreamAdmin(Configuration hConf, CConfiguration cConf, LocationFactory locationFactory,
                          HBaseTableUtil tableUtil, DatasetFramework datasetFramework,
                          TransactionExecutorFactory txExecutorFactory) throws IOException {
    super(hConf, cConf, locationFactory, tableUtil,
          datasetFramework, txExecutorFactory, QueueConstants.QueueType.STREAM);
    this.txExecutorFactory = txExecutorFactory;
  }

  @Override
  public TableId getDataTableId(QueueName queueName) {
    // tableName = system.stream.<stream name>
    if (queueName.isStream()) {
      String tableName = unqualifiedTableNamePrefix + "." + queueName.getSecondComponent();
      return TableId.from(queueName.getFirstComponent(), tableName);
    } else {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a stream.");
    }
  }

  @Override
  public boolean doDropTable(QueueName queueName) {
    // separate table for each stream, ok to drop
    return true;
  }

  @Override
  public boolean doTruncateTable(QueueName queueName) {
    // separate table for each stream, ok to truncate
    return true;
  }

  @Override
  protected List<? extends Class<? extends Coprocessor>> getCoprocessors() {
    // we don't want eviction CP here, hence overriding
    return ImmutableList.of(tableUtil.getDequeueScanObserverClassForVersion());
  }

  @Override
  public void dropAllInNamespace(Id.Namespace namespace) throws Exception {
    dropAllInNamespace(namespace.getId());
  }

  @Override
  public void configureInstances(Id.Stream streamId, final long groupId, final int instances) throws Exception {
    // TODO: The HBase stream is actually not used anymore. We have to decide what to do with this class.
    // Probably we have to modify StreamAdmin in the same way as QueueAdmin does (CDAP-1810)
    throw new UnsupportedOperationException("Configuration of consumer instances not supported");
  }

  @Override
  public void configureGroups(Id.Stream streamId, final Map<Long, Integer> groupInfo) throws Exception {
    // TODO: The HBase stream is actually not used anymore. We have to decide what to do with this class
    // Probably we have to modify StreamAdmin in the same way as QueueAdmin does (CDAP-1810)
    throw new UnsupportedOperationException("Configuration of consumer instances not supported");
  }

  @Override
  public StreamConfig getConfig(Id.Stream streamId) throws IOException {
    throw new UnsupportedOperationException("Stream config not supported for non-file based stream.");
  }

  @Override
  public void updateConfig(Id.Stream streamId, StreamProperties properties) throws IOException {
    throw new UnsupportedOperationException("Stream config not supported for non-file based stream.");
  }

  @Override
  public boolean exists(Id.Stream streamId) throws Exception {
    return exists(QueueName.fromStream(streamId));
  }

  @Override
  public void create(Id.Stream streamId) throws Exception {
    create(QueueName.fromStream(streamId));
  }

  @Override
  public void create(Id.Stream streamId, @Nullable Properties props) throws Exception {
    create(QueueName.fromStream(streamId), props);
  }

  @Override
  public void truncate(Id.Stream streamId) throws Exception {
    truncate(QueueName.fromStream(streamId));
  }

  @Override
  public void drop(Id.Stream streamId) throws Exception {
    drop(QueueName.fromStream(streamId));
  }

}
