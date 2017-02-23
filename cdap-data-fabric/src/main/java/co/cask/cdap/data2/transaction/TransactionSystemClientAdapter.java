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

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.Constants;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.thrift.TException;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;

/**
 * Translates exceptions thrown by Tephra's TransactionServiceClient when the tx service is unavailable into
 * CDAP's ServiceUnavailableException.
 */
public class TransactionSystemClientAdapter implements TransactionSystemClient {

  private final TransactionSystemClient delegate;

  public TransactionSystemClientAdapter(TransactionSystemClient delegate) {
    this.delegate = delegate;
  }

  @Override
  public Transaction startShort() {
    try {
      return delegate.startShort();
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public Transaction startShort(int timeout) {
    try {
      return delegate.startShort(timeout);
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public Transaction startLong() {
    try {
      return delegate.startLong();
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    try {
      return delegate.canCommit(tx, changeIds);
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    try {
      return delegate.commit(tx);
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public void abort(Transaction tx) {
    try {
      delegate.abort(tx);
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public boolean invalidate(long tx) {
    try {
      return delegate.invalidate(tx);
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public Transaction checkpoint(Transaction tx) throws TransactionNotInProgressException {
    try {
      return delegate.checkpoint(tx);
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    try {
      return delegate.getSnapshotInputStream();
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public String status() {
    try {
      return delegate.status();
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public void resetState() {
    try {
      delegate.resetState();
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    try {
      return delegate.truncateInvalidTx(invalidTxIds);
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    try {
      return delegate.truncateInvalidTxBefore(time);
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  @Override
  public int getInvalidSize() {
    try {
      return delegate.getInvalidSize();
    } catch (RuntimeException e) {
      throw handleException(e);
    }
  }

  private RuntimeException handleException(RuntimeException e) {
    Throwable cause = e.getCause();
    if (cause != null && cause instanceof TException) {
      throw new ServiceUnavailableException(Constants.Service.TRANSACTION, cause);
    }
    throw e;
  }
}
