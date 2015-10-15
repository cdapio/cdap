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

import co.cask.cdap.data2.transaction.DynamicTransactionExecutor;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;

/**
 * Module to implement DynamicTransactionExecutorFactory.
 * TODO (CDAP-3988): this should go away after TEPHRA-137.
 */
public class TransactionExecutorModule extends AbstractModule {

  @Override
  public void configure() {
    bind(TransactionExecutorFactory.class).to(DynamicTransactionExecutorFactory.class);
  }

  private static class DynamicTransactionExecutorFactory implements TransactionExecutorFactory {

    private final TransactionSystemClient txClient;

    @Inject
    DynamicTransactionExecutorFactory(TransactionSystemClient txClient) {
      this.txClient = txClient;
    }

    @Override
    public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
      return new DefaultTransactionExecutor(txClient, txAwares);
    }

    @Override
    public TransactionExecutor createExecutor(Supplier<TransactionContext> txContextSupplier) {
      return new DynamicTransactionExecutor(txContextSupplier);
    }
  }
}
