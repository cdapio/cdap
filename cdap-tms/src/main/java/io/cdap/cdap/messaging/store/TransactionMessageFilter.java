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

package co.cask.cdap.messaging.store;

import org.apache.tephra.Transaction;

import java.util.Arrays;

/**
 * A {@link MessageFilter} that filter {@link MessageTable.Entry} based on a given transaction.
 */
public class TransactionMessageFilter extends MessageFilter<MessageTable.Entry> {

  private final Transaction transaction;

  public TransactionMessageFilter(Transaction transaction) {
    this.transaction = transaction;
  }

  @Override
  public Result apply(MessageTable.Entry entry) {
    if (!entry.isTransactional()) {
      return Result.ACCEPT;
    }
    return filter(entry.getTransactionWritePointer());
  }

  public Result filter(long txWritePtr) {
    // This transaction has been rolled back and thus skip the entry
    if (txWritePtr < 0) {
      return Result.SKIP;
    }

    // This transaction is visible, hence accept the message
    if (transaction.isVisible(txWritePtr)) {
      return Result.ACCEPT;
    }

    // This transaction is an invalid transaction, so skip the entry and proceed to the next
    if (Arrays.binarySearch(transaction.getInvalids(), txWritePtr) >= 0) {
      return Result.SKIP;
    }

    // This transaction has not yet been committed, hence hold to ensure ordering
    return Result.HOLD;
  }
}
