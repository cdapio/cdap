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
package co.cask.cdap.data2.transaction.queue;

import co.cask.tephra.Transaction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * For performing queue eviction action.
 */
public interface QueueEvictor {

  /**
   * Performs queue eviction in the given transaction context.
   * @param transaction The transaction context that the eviction happens.
   * @return A {@link ListenableFuture} that will be completed when eviction is done. The future result
   *         carries number of entries being evicted.
   */
  ListenableFuture<Integer> evict(Transaction transaction);

  static final QueueEvictor NOOP = new QueueEvictor() {

    @Override
    public ListenableFuture<Integer> evict(Transaction transaction) {
      return Futures.immediateFuture(0);
    }
  };
}
