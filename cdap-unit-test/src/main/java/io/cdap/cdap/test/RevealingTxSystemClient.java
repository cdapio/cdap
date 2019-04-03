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

package co.cask.cdap.test;

import com.google.inject.Inject;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;

/**
 * A TransactionSystemClient that creates transaction objects with additional fields for validation.
 */
public class RevealingTxSystemClient implements TransactionSystemClient {

  private final InMemoryTxSystemClient txClient;

  @Inject
  RevealingTxSystemClient(InMemoryTxSystemClient txClient) {
    this.txClient = txClient;
  }

  @Override
  public Transaction startLong() {
    return txClient.startLong();
  }

  @Override
  public Transaction startShort() {
    return txClient.startShort();
  }

  @Override
  public Transaction startShort(int timeout) {
    return new RevealingTransaction(txClient.startShort(timeout), timeout);
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    //noinspection deprecation
    return txClient.canCommit(tx, changeIds);
  }

  @Override
  public void canCommitOrThrow(Transaction tx, Collection<byte[]> changeIds) throws TransactionFailureException {
    txClient.canCommitOrThrow(tx, changeIds);
  }

  @Override
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    //noinspection deprecation
    return txClient.commit(tx);
  }

  @Override
  public void commitOrThrow(Transaction tx) throws TransactionFailureException {
    txClient.commitOrThrow(tx);
  }

  @Override
  public void abort(Transaction tx) {
    txClient.abort(tx);
  }

  @Override
  public boolean invalidate(long tx) {
    return txClient.invalidate(tx);
  }

  @Override
  public Transaction checkpoint(Transaction tx) throws TransactionNotInProgressException {
    return txClient.checkpoint(tx);
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    return txClient.getSnapshotInputStream();
  }

  @Override
  public String status() {
    return txClient.status();
  }

  @Override
  public void resetState() {
    txClient.resetState();
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    return txClient.truncateInvalidTx(invalidTxIds);
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    return txClient.truncateInvalidTxBefore(time);
  }

  @Override
  public int getInvalidSize() {
    return txClient.getInvalidSize();
  }

  @Override
  public void pruneNow() {
    txClient.pruneNow();
  }

  /**
   * This transaction class allows us to return additional details about the transaction to the client.
   * For now, it is only the transaction timeout, but we can add more information later.
   * This can then be used by test cases to validate that the transaction was started with the right properties.
   */
  public static class RevealingTransaction extends Transaction {

    // making this field public: Because this will be loaded in a different classloader than the test app,
    // the app cannot cast to this class and call a getter. Since it will have to use reflection anyway, it may
    // as well access the field directly.
    public final int timeout;

    RevealingTransaction(Transaction tx, int timeout) {
      super(tx, tx.getWritePointer(), tx.getCheckpointWritePointers());
      this.timeout = timeout;
    }

    @Override
    public String toString() {
      return super.toString() + ",timeout=" + timeout;
    }
  }
}
