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

package co.cask.cdap.data.stream;

import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.data.runtime.InMemoryStreamFileWriterFactory;
import co.cask.cdap.data.runtime.LocationStreamFileWriterFactory;
import co.cask.cdap.data.stream.service.MDSStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.hbase.HBaseStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.hbase.HBaseStreamFileAdmin;
import co.cask.cdap.data2.transaction.stream.hbase.HBaseStreamFileConsumerFactory;
import co.cask.cdap.data2.transaction.stream.inmemory.InMemoryStreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.inmemory.InMemoryStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.inmemory.InMemoryStreamFileAdmin;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileAdmin;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Singleton;

/**
 * Guice modules to have access to a {@link StreamAdmin} implementation.
 */
public class StreamAdminModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(StreamAdmin.class).to(InMemoryStreamFileAdmin.class).in(Singleton.class);
        bind(StreamCoordinatorClient.class).to(InMemoryStreamCoordinatorClient.class).in(Singleton.class);
        bind(StreamMetaStore.class).to(MDSStreamMetaStore.class).in(Singleton.class);
        bind(StreamConsumerFactory.class).to(InMemoryStreamConsumerFactory.class).in(Singleton.class);
        bind(StreamConsumerStateStoreFactory.class)
          .to(InMemoryStreamConsumerStateStoreFactory.class).in(Singleton.class);
        bind(StreamFileWriterFactory.class).to(InMemoryStreamFileWriterFactory.class).in(Singleton.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(StreamCoordinatorClient.class).to(InMemoryStreamCoordinatorClient.class).in(Singleton.class);
        bind(StreamMetaStore.class).to(MDSStreamMetaStore.class).in(Singleton.class);
        bind(StreamConsumerStateStoreFactory.class)
          .to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
        bind(StreamAdmin.class).to(LevelDBStreamFileAdmin.class).in(Singleton.class);
        bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
        bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(StreamCoordinatorClient.class).to(DistributedStreamCoordinatorClient.class).in(Singleton.class);
        bind(StreamMetaStore.class).to(MDSStreamMetaStore.class).in(Singleton.class);
        bind(StreamConsumerStateStoreFactory.class).to(HBaseStreamConsumerStateStoreFactory.class).in(Singleton.class);
        bind(StreamAdmin.class).to(HBaseStreamFileAdmin.class).in(Singleton.class);
        bind(StreamConsumerFactory.class).to(HBaseStreamFileConsumerFactory.class).in(Singleton.class);
        bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);
      }
    };
  }
}
