/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;

/**
 * An implementation of {@link TransactionSystemClient} that throws {@link UnsupportedOperationException} on
 * every method call. This is used in runtime environment that transaction is not supported.
 */
final class UnsupportedTransactionSystemClient implements TransactionSystemClient {

  @Override
  public Transaction startShort() {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public Transaction startShort(int timeout) {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public Transaction startLong() {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public void canCommitOrThrow(Transaction tx, Collection<byte[]> changeIds) throws TransactionFailureException {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public void commitOrThrow(Transaction tx) throws TransactionFailureException {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public void abort(Transaction tx) {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public boolean invalidate(long tx) {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public Transaction checkpoint(Transaction tx) throws TransactionNotInProgressException {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public String status() {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public void resetState() {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public int getInvalidSize() {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }

  @Override
  public void pruneNow() {
    throw new UnsupportedOperationException("Transaction operation is not supported");
  }
}
