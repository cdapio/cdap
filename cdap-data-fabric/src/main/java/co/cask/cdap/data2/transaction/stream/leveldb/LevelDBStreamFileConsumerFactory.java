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
import co.cask.cdap.data.file.FileReader;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.data.stream.StreamEventOffset;
import co.cask.cdap.data.stream.StreamFileOffset;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.transaction.stream.AbstractStreamFileConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumerState;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;

/**
 * A {@link co.cask.cdap.data2.transaction.stream.StreamConsumerFactory} that reads from stream file
 * and uses LevelDB as the store for consumer process states.
 */
public final class LevelDBStreamFileConsumerFactory extends AbstractStreamFileConsumerFactory {

  private final CConfiguration cConf;
  private final LevelDBTableService tableService;
  private final ConcurrentMap<String, Object> dbLocks;

  @Inject
  LevelDBStreamFileConsumerFactory(StreamAdmin streamAdmin,
                                   StreamConsumerStateStoreFactory stateStoreFactory,
                                   CConfiguration cConf, LevelDBTableService tableService) {
    super(cConf, streamAdmin, stateStoreFactory);
    this.cConf = cConf;
    this.tableService = tableService;
    this.dbLocks = Maps.newConcurrentMap();
  }


  @Override
  protected StreamConsumer create(String tableName, StreamConfig streamConfig, ConsumerConfig consumerConfig,
                                  StreamConsumerStateStore stateStore, StreamConsumerState beginConsumerState,
                                  FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                                  @Nullable ReadFilter extraFilter) throws IOException {

    tableService.ensureTableExists(tableName);

    LevelDBTableCore tableCore = new LevelDBTableCore(tableName, tableService);
    Object dbLock = getDBLock(tableName);
    return new LevelDBStreamFileConsumer(cConf, streamConfig, consumerConfig, reader,
                                         stateStore, beginConsumerState, extraFilter,
                                         tableCore, dbLock);
  }

  @Override
  protected void dropTable(String tableName) throws IOException {
    tableService.dropTable(tableName);
  }

  private Object getDBLock(String name) {
    Object lock = dbLocks.get(name);
    if (lock == null) {
      lock = new Object();
      Object existing = dbLocks.putIfAbsent(name, lock);
      if (existing != null) {
        lock = existing;
      }
    }
    return lock;

  }
}
