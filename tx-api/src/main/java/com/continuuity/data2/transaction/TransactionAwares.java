/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility class that provides handy methods for working with {@link TransactionAware} classes and their instances.
 */
public final class TransactionAwares {
  private TransactionAwares() {}

  /**
   * Create composite transaction aware object that delegates transaction logic to given collection of
   * {@link TransactionAware}s
   * @param transactionAwares collection of {@link TransactionAware}s
   * @return instance of {@link TransactionAware}
   */
  public static TransactionAware of(Collection<TransactionAware> transactionAwares) {
    // this is most common case, trying to optimize
    if (transactionAwares.size() == 1) {
      return transactionAwares.iterator().next();
    }

    TransactionAwareCollection result = new TransactionAwareCollection();
    result.addAll(transactionAwares);

    return result;
  }

  private static class TransactionAwareCollection extends ArrayList<TransactionAware> implements TransactionAware {

    @Override
    public void startTx(Transaction tx) {
      for (TransactionAware txAware : this) {
        txAware.startTx(tx);
      }
    }

    @Override
    public Collection<byte[]> getTxChanges() {
      List<byte[]> changes = new ArrayList<byte[]>();
      for (TransactionAware txAware : this) {
        changes.addAll(txAware.getTxChanges());
      }

      return changes;
    }

    @Override
    public boolean commitTx() throws Exception {
      boolean success = true;
      for (TransactionAware txAware : this) {
        success = success && txAware.commitTx();
      }
      return success;
    }

    @Override
    public void postTxCommit() {
      for (TransactionAware txAware : this) {
        txAware.postTxCommit();
      }
    }

    @Override
    public boolean rollbackTx() throws Exception {
      boolean success = true;
      for (TransactionAware txAware : this) {
        success = success && txAware.rollbackTx();
      }
      return success;
    }

    @Override
    public String getName() {
      // todo: will go away, see comment at TransactionAware
      StringBuilder sb = new StringBuilder("{");
      for (TransactionAware txAware : this) {
        sb.append(txAware.getName()).append(",");
      }
      sb.replace(sb.length() - 1, sb.length() - 1, "}");
      return sb.toString();
    }
  }
}
