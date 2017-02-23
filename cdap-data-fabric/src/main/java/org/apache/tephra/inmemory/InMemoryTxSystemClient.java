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

package org.apache.tephra.inmemory;

import com.google.inject.Inject;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Set;

/**
 *
 */
public class InMemoryTxSystemClient implements TransactionSystemClient {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTxSystemClient.class);

  TransactionManager txManager;

  @Inject
  public InMemoryTxSystemClient(TransactionManager txmgr) {
    txManager = txmgr;
  }

  @Override
  public Transaction startLong() {
    return txManager.startLong();
  }

  @Override
  public Transaction startShort() {
    return txManager.startShort();
  }

  @Override
  public Transaction startShort(int timeout) {
    return txManager.startShort(timeout);
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    return changeIds.isEmpty() || txManager.canCommit(tx, changeIds);
  }

  @Override
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    return txManager.commit(tx);
  }

  @Override
  public void abort(Transaction tx) {
    txManager.abort(tx);
  }

  @Override
  public boolean invalidate(long tx) {
    return txManager.invalidate(tx);
  }

  @Override
  public Transaction checkpoint(Transaction tx) throws TransactionNotInProgressException {
    return txManager.checkpoint(tx);
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        boolean snapshotTaken = txManager.takeSnapshot(out);
        if (!snapshotTaken) {
          throw new TransactionCouldNotTakeSnapshotException("Transaction manager did not take a snapshot.");
        }
      } finally {
        out.close();
      }
      return new ByteArrayInputStream(out.toByteArray());
    } catch (IOException e) {
      LOG.error("Snapshot could not be taken", e);
      throw new TransactionCouldNotTakeSnapshotException(e.getMessage());
    }
  }

  @Override
  public String status() {
    return txManager.isRunning() ? TxConstants.STATUS_OK : TxConstants.STATUS_NOTOK;
  }

  @Override
  public void resetState() {
    txManager.resetState();
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    return txManager.truncateInvalidTx(invalidTxIds);
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    return txManager.truncateInvalidTxBefore(time);
  }

  @Override
  public int getInvalidSize() {
    return txManager.getInvalidSize();
  }
}
