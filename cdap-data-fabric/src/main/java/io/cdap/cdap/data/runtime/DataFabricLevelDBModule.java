/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
package io.cdap.cdap.data.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.data2.transaction.metrics.TransactionManagerMetricsCollector;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.metrics.MetricsCollector;
import org.apache.tephra.persist.LocalFileTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.runtime.TransactionStateStorageProvider;
import org.apache.tephra.snapshot.SnapshotCodecProvider;

import java.lang.management.ManagementFactory;

/**
 * DataFabricLocalModule defines the Local/HyperSQL bindings for the data fabric.
 */
public class DataFabricLevelDBModule extends AbstractModule {

  @Override
  public void configure() {
    bind(LevelDBTableService.class).in(Scopes.SINGLETON);

    // bind transactions
    bind(TransactionSystemClientService.class).to(DelegatingTransactionSystemClientService.class);

    bind(SnapshotCodecProvider.class).in(Scopes.SINGLETON);
    bind(TransactionStateStorage.class).annotatedWith(Names.named("persist"))
      .to(LocalFileTransactionStateStorage.class).in(Scopes.SINGLETON);
    bind(TransactionStateStorage.class).toProvider(TransactionStateStorageProvider.class).in(Scopes.SINGLETON);
    bind(TransactionManager.class).in(Scopes.SINGLETON);

    bindConstant().annotatedWith(Names.named(TxConstants.CLIENT_ID)).to(ManagementFactory.getRuntimeMXBean().getName());

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));

    // Binds the tephra MetricsCollector to the one that emit metrics via MetricsCollectionService
    bind(MetricsCollector.class).to(TransactionManagerMetricsCollector.class).in(Scopes.SINGLETON);
    bind(TransactionSystemClient.class).toProvider(
      DataFabricInMemoryModule.InMemoryTransactionSystemClientProvider.class).in(Scopes.SINGLETON);

    install(new TransactionExecutorModule());
    install(new StorageModule());
  }
}
