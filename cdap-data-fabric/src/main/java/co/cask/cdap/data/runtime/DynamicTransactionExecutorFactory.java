/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.data2.transaction.TransactionContextFactory;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionSystemClient;

/**
 * Implementation of {@link TransactionExecutorFactory} that supports dynamic creation of {@link TransactionContext}
 * to be used for the transactional execution.
 */
public class DynamicTransactionExecutorFactory implements TransactionExecutorFactory {

  private final TransactionSystemClient txClient;

  @VisibleForTesting
  @Inject
  public DynamicTransactionExecutorFactory(TransactionSystemClient txClient) {
    this.txClient = txClient;
  }

  @Override
  public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
    return new DefaultTransactionExecutor(txClient, txAwares);
  }

  @Override
  public TransactionExecutor createExecutor(TransactionContextFactory txContextFactory) {
    return new DynamicTransactionExecutor(txContextFactory);
  }
}
