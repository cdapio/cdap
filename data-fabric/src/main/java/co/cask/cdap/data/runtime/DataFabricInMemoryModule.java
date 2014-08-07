/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.data.stream.InMemoryStreamCoordinator;
import co.cask.cdap.data.stream.StreamCoordinator;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.metrics.TransactionManagerMetricsCollector;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.inmemory.InMemoryQueueAdmin;
import co.cask.cdap.data2.transaction.queue.inmemory.InMemoryQueueClientFactory;
import co.cask.cdap.data2.transaction.queue.inmemory.InMemoryStreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.inmemory.InMemoryStreamConsumerFactory;
import com.continuuity.tephra.metrics.TxMetricsCollector;
import com.continuuity.tephra.runtime.TransactionModules;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

/**
 * The Guice module of data fabric bindings for in memory execution.
 */
public class DataFabricInMemoryModule extends AbstractModule {

  @Override
  protected void configure() {
    // Bind TxDs2 stuff

    bind(QueueClientFactory.class).to(InMemoryQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(InMemoryQueueAdmin.class).in(Singleton.class);
    bind(StreamAdmin.class).to(InMemoryStreamAdmin.class).in(Singleton.class);

    bind(StreamCoordinator.class).to(InMemoryStreamCoordinator.class).in(Singleton.class);
    bind(StreamConsumerFactory.class).to(InMemoryStreamConsumerFactory.class).in(Singleton.class);
    bind(StreamFileWriterFactory.class).to(InMemoryStreamFileWriterFactory.class).in(Singleton.class);

    // bind transactions
    bind(TxMetricsCollector.class).to(TransactionManagerMetricsCollector.class).in(Scopes.SINGLETON);
    install(new TransactionModules().getInMemoryModules());
  }
}
