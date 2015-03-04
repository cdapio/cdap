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

package co.cask.cdap.data2.transaction.queue.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamProperties;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * admin for streams in leveldb.
 */
@Singleton
public class LevelDBStreamAdmin extends LevelDBQueueAdmin implements StreamAdmin {

  @Inject
  public LevelDBStreamAdmin(CConfiguration conf, LevelDBTableService service) {
    super(conf, service, QueueConstants.QueueType.STREAM);
  }

  @Override
  public TableId getDataTableId(QueueName queueName) {
    // tableName = system.stream.<stream name>
    if (queueName.isStream()) {
      String tableName = unqualifiedTableNamePrefix + "." + queueName.getSecondComponent();
      return TableId.from(root, queueName.getFirstComponent(), tableName);
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
  public void dropAllInNamespace(Id.Namespace namespace) throws Exception {
    dropAllInNamespace(namespace.getId());
  }

  @Override
  public void configureInstances(Id.Stream streamId, long groupId, int instances) throws Exception {
    configureInstances(QueueName.fromStream(streamId), groupId, instances);
  }

  @Override
  public void configureGroups(Id.Stream streamId, Map<Long, Integer> groupInfo) throws Exception {
    configureGroups(QueueName.fromStream(streamId), groupInfo);
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
  public long fetchStreamSize(StreamConfig streamConfig) throws IOException {
    throw new UnsupportedOperationException("Not yet supported");
  }

  private String fromStream(Id.Stream streamId) {
    return QueueName.fromStream(streamId).toURI().toString();
  }

  @Override
  public boolean exists(Id.Stream streamId) throws Exception {
    return exists(fromStream(streamId));
  }

  @Override
  public void create(Id.Stream streamId) throws Exception {
    create(fromStream(streamId));
  }

  @Override
  public void create(Id.Stream streamId, @Nullable Properties props) throws Exception {
    create(fromStream(streamId), props);
  }

  @Override
  public void truncate(Id.Stream streamId) throws Exception {
    truncate(fromStream(streamId));
  }

  @Override
  public void drop(Id.Stream streamId) throws Exception {
    drop(fromStream(streamId));
  }

}
