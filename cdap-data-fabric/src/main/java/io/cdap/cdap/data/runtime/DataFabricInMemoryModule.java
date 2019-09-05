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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.data2.transaction.UninterruptibleTransactionSystemClient;
import io.cdap.cdap.data2.transaction.metrics.TransactionManagerMetricsCollector;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.metrics.MetricsCollector;
import org.apache.tephra.persist.NoOpTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.snapshot.SnapshotCodecProvider;

/**
 * The Guice module of data fabric bindings for in memory execution.
 */
public class DataFabricInMemoryModule extends AbstractModule {
  private final String txClientId;

  public DataFabricInMemoryModule(String txClientId) {
    this.txClientId = txClientId;
  }

  @Override
  protected void configure() {
    // bind transactions
    bind(TransactionSystemClientService.class).to(DelegatingTransactionSystemClientService.class);
    bind(SnapshotCodecProvider.class).in(Scopes.SINGLETON);
    bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class).in(Scopes.SINGLETON);
    bind(TransactionManager.class).in(Scopes.SINGLETON);

    bindConstant().annotatedWith(Names.named(TxConstants.CLIENT_ID)).to(txClientId);
    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));

    // Binds the tephra MetricsCollector to the one that emit metrics via MetricsCollectionService
    bind(MetricsCollector.class).to(TransactionManagerMetricsCollector.class).in(Scopes.SINGLETON);
    bind(TransactionSystemClient.class).toProvider(InMemoryTransactionSystemClientProvider.class).in(Scopes.SINGLETON);

    install(new TransactionExecutorModule());
    install(new StorageModule());
  }

  /**
   * In memory transaction client provider which provides the {@link TransactionSystemClient} for in-memory mode.
   */
  static final class InMemoryTransactionSystemClientProvider extends AbstractTransactionSystemClientProvider {
    private final Injector injector;

    @Inject
    InMemoryTransactionSystemClientProvider(CConfiguration cConf, Injector injector) {
      super(cConf);
      this.injector = injector;
    }

    @Override
    protected TransactionSystemClient getTransactionSystemClient() {
      return new UninterruptibleTransactionSystemClient(injector.getInstance(InMemoryTxSystemClient.class));
    }
  }
}
