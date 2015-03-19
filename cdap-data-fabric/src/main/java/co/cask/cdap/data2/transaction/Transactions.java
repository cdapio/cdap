/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for interacting with {@link Transaction} and {@link TransactionSystemClient}.
 */
public final class Transactions {

  private static final Logger LOG = LoggerFactory.getLogger(Transactions.class);

  public static void abortQuietly(TransactionSystemClient txClient, Transaction tx) {
    try {
      txClient.abort(tx);
    } catch (Throwable t) {
      LOG.error("Exception when aborting transaction {}", tx, t);
    }
  }

  public static void invalidateQuietly(TransactionSystemClient txClient, Transaction tx) {
    try {
      txClient.invalidate(tx.getWritePointer());
    } catch (Throwable t) {
      LOG.error("Exception when invalidating transaction {}", tx, t);
    }
  }

  /**
   * Handy method to create {@link TransactionExecutor} (See TEPHRA-71).
   */
  public static TransactionExecutor createTransactionExecutor(TransactionExecutorFactory factory,
                                                              Iterable<? extends TransactionAware> txAwares) {
    return factory.createExecutor(Iterables.transform(txAwares, Functions.<TransactionAware>identity()));
  }

  /**
   * Handy method to create {@link TransactionExecutor} with single {@link TransactionAware}.
   */
  public static TransactionExecutor createTransactionExecutor(TransactionExecutorFactory factory,
                                                              TransactionAware txAware) {
    return factory.createExecutor(ImmutableList.of(txAware));
  }

  private Transactions() {
    // Private the constructor for util class
  }
}
