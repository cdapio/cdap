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

package org.apache.tephra.distributed;

import com.google.common.collect.Sets;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.thrift.TBoolean;
import org.apache.tephra.distributed.thrift.TGenericException;
import org.apache.tephra.distributed.thrift.TInvalidTruncateTimeException;
import org.apache.tephra.distributed.thrift.TTransaction;
import org.apache.tephra.distributed.thrift.TTransactionCouldNotTakeSnapshotException;
import org.apache.tephra.distributed.thrift.TTransactionNotInProgressException;
import org.apache.tephra.distributed.thrift.TTransactionServer;
import org.apache.tephra.rpc.RPCServiceHandler;
import org.apache.thrift.TException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * The implementation of a thrift service for tx service.
 * All operations arrive over the wire as Thrift objects.
 * <p/>
 * Why all this conversion (wrap/unwrap), and not define all Operations
 * themselves as Thrift objects?
 * <ul><li>
 * All the non-thrift executors would have to use the Thrift objects
 * </li><li>
 * Thrift's object model is too restrictive: it has only limited inheritance
 * and no overloading
 * </li><li>
 * Thrift objects are bare-bone, all they have are getters, setters, and
 * basic object methods.
 * </li></ul>
 */
public class TransactionServiceThriftHandler implements TTransactionServer.Iface, RPCServiceHandler {

  private final TransactionManager txManager;

  public TransactionServiceThriftHandler(TransactionManager txManager) {
    this.txManager = txManager;
  }

  @Override
  public TTransaction startLong() throws TException {
    return TransactionConverterUtils.wrap(txManager.startLong());
  }

  @Override
  public TTransaction startShort() throws TException {
    return TransactionConverterUtils.wrap(txManager.startShort());
  }

  @Override
  public TTransaction startShortTimeout(int timeout) throws TException {
    return TransactionConverterUtils.wrap(txManager.startShort(timeout));
  }

  @Override
  public TTransaction startShortWithTimeout(int timeout) throws TException {
    try {
      return TransactionConverterUtils.wrap(txManager.startShort(timeout));
    } catch (IllegalArgumentException e) {
      throw new TGenericException(e.getMessage(), e.getClass().getName());
    }
  }

  @Override
  public TBoolean canCommitTx(TTransaction tx, Set<ByteBuffer> changes) throws TException {

    Set<byte[]> changeIds = Sets.newHashSet();
    for (ByteBuffer bb : changes) {
      byte[] changeId = new byte[bb.remaining()];
      bb.get(changeId);
      changeIds.add(changeId);
    }
    try {
      return new TBoolean(txManager.canCommit(TransactionConverterUtils.unwrap(tx), changeIds));
    } catch (TransactionNotInProgressException e) {
      throw new TTransactionNotInProgressException(e.getMessage());
    }
  }

  @Override
  public TBoolean commitTx(TTransaction tx) throws TException {
    try {
      return new TBoolean(txManager.commit(TransactionConverterUtils.unwrap(tx)));
    } catch (TransactionNotInProgressException e) {
      throw new TTransactionNotInProgressException(e.getMessage());
    }
  }

  @Override
  public void abortTx(TTransaction tx) throws TException {
    txManager.abort(TransactionConverterUtils.unwrap(tx));
  }

  @Override
  public boolean invalidateTx(long tx) throws TException {
    return txManager.invalidate(tx);
  }

  @Override
  public ByteBuffer getSnapshot() throws TException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        boolean snapshotTaken = txManager.takeSnapshot(out);
        if (!snapshotTaken) {
          throw new TTransactionCouldNotTakeSnapshotException("Transaction manager could not get a snapshot.");
        }
      } finally {
        out.close();
      }
      // todo find a way to encode directly to the stream, without having the snapshot in memory twice
      return ByteBuffer.wrap(out.toByteArray());
    } catch (IOException e) {
      throw new TTransactionCouldNotTakeSnapshotException(e.getMessage());
    }
  }

  @Override
  public void resetState() throws TException {
    txManager.resetState();
  }

  @Override
  public String status() throws TException {
    return txManager.isRunning() ? TxConstants.STATUS_OK : TxConstants.STATUS_NOTOK;
  }

  @Override
  public TBoolean truncateInvalidTx(Set<Long> txns) throws TException {
    return new TBoolean(txManager.truncateInvalidTx(txns));
  }

  @Override
  public TBoolean truncateInvalidTxBefore(long time) throws TException {
    try {
      return new TBoolean(txManager.truncateInvalidTxBefore(time));
    } catch (InvalidTruncateTimeException e) {
      throw new TInvalidTruncateTimeException(e.getMessage());
    }
  }

  @Override
  public int invalidTxSize() throws TException {
    return txManager.getInvalidSize();
  }

  @Override
  public TTransaction checkpoint(TTransaction originalTx) throws TException {
    try {
      return TransactionConverterUtils.wrap(
          txManager.checkpoint(TransactionConverterUtils.unwrap(originalTx)));
    } catch (TransactionNotInProgressException e) {
      throw new TTransactionNotInProgressException(e.getMessage());
    }
  }

  /* RPCServiceHandler implementation */

  @Override
  public void init() throws Exception {
    txManager.startAndWait();
  }

  @Override
  public void destroy() throws Exception {
    txManager.stopAndWait();
  }
}
