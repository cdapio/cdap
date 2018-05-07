/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction;

import co.cask.cdap.common.service.RetryStrategy;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionSystemClient;

/**
 * Retries only the calls to start a transaction.
 */
public class RetryingShortTransactionSystemClient extends RetryingTransactionSystemClient {

  public RetryingShortTransactionSystemClient(TransactionSystemClient delegate, RetryStrategy retryStrategy) {
    super(delegate, retryStrategy);
  }

  @Override
  public Transaction startShort() {
    return supplyWithRetries(delegate::startShort);
  }

  @Override
  public Transaction startShort(final int timeout) {
    return supplyWithRetries(() -> delegate.startShort(timeout));
  }
}
