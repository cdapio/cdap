/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.visibility;

import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.util.concurrent.TimeoutException;

/**
 * VisibilityFence is used to ensure that after a given point in time, all readers see an updated change
 * that got committed.
 * <p>
 *
 * Typically a reader will never conflict with a writer, since a reader only sees committed changes when its
 * transaction started. However to ensure that after a given point all readers are aware of a change,
 * we have to introduce a conflict between a reader and a writer that act on the same data concurrently.
 *<p>
 *
 * This is done by the reader indicating that it is interested in changes to a piece of data by using a fence
 * in its transaction. If there are no changes to the data when reader tries to commit the transaction
 * containing the fence, the commit succeeds.
 * <p>
 *
 * On the other hand, a writer updates the same data in a transaction. After the write transaction is committed,
 * the writer then waits on the fence to ensure that all in-progress readers are aware of this update.
 * When the wait on the fence returns successfully, it means that
 * any in-progress readers that have not seen the change will not be allowed to commit anymore. This will
 * force the readers to start a new transaction, and this ensures that the changes made by writer are visible
 * to the readers.
 *<p>
 *
 * In case an in-progress reader commits when the writer is waiting on the fence, then the wait method will retry
 * until the given timeout.
 * <p>
 *
 * Hence a successful await on a fence ensures that any reader (using the same fence) that successfully commits after
 * this point onwards would see the change.
 *
 * <p>
 * Sample reader code:
 * <pre>
 *   <code>
 * TransactionAware fence = VisibilityFence.create(fenceId);
 * TransactionContext readTxContext = new TransactionContext(txClient, fence, table1, table2, ...);
 * readTxContext.start();
 *
 * // do operations using table1, table2, etc.
 *
 * // finally commit
 * try {
 *   readTxContext.finish();
 * } catch (TransactionConflictException e) {
 *   // handle conflict by aborting and starting over with a new transaction
 * }
 *   </code>
 * </pre>
 *<p>
 *
 * Sample writer code:
 * <pre>
 *   <code>
 * // start transaction
 * // write change
 * // commit transaction
 *
 * // Now wait on the fence (with the same fenceId as the readers) to ensure that all in-progress readers are
 * aware of this change
 * try {
 *   FenceWait fenceWait = VisibilityFence.prepareWait(fenceId, txClient);
 *   fenceWait.await(50000, 50, TimeUnit.MILLISECONDS);
 * } catch (TimeoutException e) {
 *   // await timed out, the change may not be visible to all in-progress readers.
 *   // Application has two options at this point:
 *   // 1. Revert the write. Re-try the write and fence wait again.
 *   // 2. Retry only the wait with the same fenceWait object (retry logic is not shown here).
 * }
 *   </code>
 * </pre>
 *
 * fenceId in the above samples refers to any id that both the readers and writer know for a given
 * piece of data. Both readers and writer will have to use the same fenceId to synchronize on a given data.
 * Typically fenceId uniquely identifies the data in question.
 * For example, if the data is a table row, the fenceId can be composed of table name and row key.
 * If the data is a table cell, the fenceId can be composed of table name, row key, and column key.
 *<p>
 *
 * Note that in this implementation, any reader that starts a transaction after the write is committed, and
 * while this read transaction is in-progress, if a writer successfully starts and completes an await on the fence then
 * this reader will get a conflict while committing the fence even though this reader has seen the latest changes.
 * This is because today there is no way to determine the commit time of a transaction.
 */
public final class VisibilityFence {
  private VisibilityFence() {
    // Cannot instantiate this class, all functionality is through static methods.
  }

  /**
   * Used by a reader to get a fence that can be added to its transaction context.
   *
   * @param fenceId uniquely identifies the data that this fence is used to synchronize.
   *                  If the data is a table cell then this id can be composed of the table name, row key
   *                  and column key for the data.
   * @return {@link TransactionAware} to be added to reader's transaction context.
   */
  public static TransactionAware create(byte[] fenceId) {
    return new ReadFence(fenceId);
  }

  /**
   * Used by a writer to wait on a fence so that changes are visible to all readers with in-progress transactions.
   *
   * @param fenceId uniquely identifies the data that this fence is used to synchronize.
   *                  If the data is a table cell then this id can be composed of the table name, row key
   *                  and column key for the data.
   * @return {@link FenceWait} object
   */
  public static FenceWait prepareWait(byte[] fenceId, TransactionSystemClient txClient)
    throws TransactionFailureException, InterruptedException, TimeoutException {
    return new DefaultFenceWait(new TransactionContext(txClient, new WriteFence(fenceId)));
  }
}
