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
package co.cask.cdap.data2.transaction.stream.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableCore;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableService;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * Factory for creating {@link StreamConsumerStateStore} in level db.
 */
public final class LevelDBStreamConsumerStateStoreFactory implements StreamConsumerStateStoreFactory {

  private final LevelDBOrderedTableService tableService;
  private final String tableName;
  private LevelDBOrderedTableCore coreTable;

  @Inject
  LevelDBStreamConsumerStateStoreFactory(CConfiguration conf, LevelDBOrderedTableService tableService) {
    this.tableService = tableService;
    this.tableName = new DefaultDatasetNamespace(conf)
      .namespace(QueueConstants.STREAM_TABLE_PREFIX + ".state.store").getId();
  }

  @Override
  public synchronized StreamConsumerStateStore create(StreamConfig streamConfig) throws IOException {
    if (coreTable == null) {
      tableService.ensureTableExists(tableName);
      coreTable = new LevelDBOrderedTableCore(tableName, tableService);
    }
    return new LevelDBStreamConsumerStateStore(streamConfig, coreTable);
  }

  @Override
  public synchronized void dropAll() throws IOException {
    coreTable = null;
    tableService.dropTable(tableName);
  }
}
