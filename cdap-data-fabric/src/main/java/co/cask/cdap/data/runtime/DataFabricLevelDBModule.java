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
package co.cask.cdap.data.runtime;

import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.InMemoryStreamCoordinator;
import co.cask.cdap.data.stream.service.MDSStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamCoordinator;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableService;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.metrics.TransactionManagerMetricsCollector;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.leveldb.LevelDBQueueAdmin;
import co.cask.cdap.data2.transaction.queue.leveldb.LevelDBQueueClientFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileAdmin;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.runtime.TransactionModules;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

/**
 * DataFabricLocalModule defines the Local/HyperSQL bindings for the data fabric.
 */
public class DataFabricLevelDBModule extends AbstractModule {

  @Override
  public void configure() {
    bind(LevelDBOrderedTableService.class).toInstance(LevelDBOrderedTableService.getInstance());

    bind(QueueClientFactory.class).to(LevelDBQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(LevelDBQueueAdmin.class).in(Singleton.class);

    // Stream bindings.
    bind(StreamCoordinator.class).to(InMemoryStreamCoordinator.class).in(Singleton.class);
    bind(StreamMetaStore.class).to(MDSStreamMetaStore.class).in(Singleton.class);

    bind(StreamConsumerStateStoreFactory.class).to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
    bind(StreamAdmin.class).to(LevelDBStreamFileAdmin.class).in(Singleton.class);
    bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
    bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);

    // bind transactions
    bind(TxMetricsCollector.class).to(TransactionManagerMetricsCollector.class).in(Scopes.SINGLETON);
    install(new TransactionModules().getInMemoryModules());
  }
}
