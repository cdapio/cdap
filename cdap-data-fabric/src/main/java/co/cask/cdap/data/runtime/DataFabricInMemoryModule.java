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
package co.cask.cdap.data.runtime;

import co.cask.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.cdap.data2.transaction.metrics.TransactionManagerMetricsCollector;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.metrics.MetricsCollector;
import org.apache.tephra.runtime.TransactionModules;

/**
 * The Guice module of data fabric bindings for in memory execution.
 */
public class DataFabricInMemoryModule extends AbstractModule {
  private final String txClientId;
  private final boolean useNoopTxClient;

  public DataFabricInMemoryModule(String txClientId, boolean useNoopTxClient) {
    this.txClientId = txClientId;
    this.useNoopTxClient = useNoopTxClient;
  }

  @Override
  protected void configure() {
    // bind transactions
    bind(TransactionSystemClientService.class).to(DelegatingTransactionSystemClientService.class);
    install(Modules.override(new TransactionModules(txClientId).getInMemoryModules()).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Binds the tephra MetricsCollector to the one that emit metrics via MetricsCollectionService
        bind(MetricsCollector.class).to(TransactionManagerMetricsCollector.class).in(Scopes.SINGLETON);
        if (useNoopTxClient) {
          bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class).in(Scopes.SINGLETON);
        }
      }
    }));
    install(new TransactionExecutorModule());
    install(new StorageModule());
  }
}
