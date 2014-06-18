package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.rpc.RPCServiceHandler;
import com.continuuity.data2.transaction.TransactionNotInProgressException;
import com.continuuity.data2.transaction.distributed.thrift.TBoolean;
import com.continuuity.data2.transaction.distributed.thrift.TTransaction;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionCouldNotTakeSnapshotException;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionNotInProgressException;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionServer;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.collect.Sets;
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
  private InMemoryTransactionManager txManager;

  public TransactionServiceThriftHandler(InMemoryTransactionManager txManager) {
    this.txManager = txManager;
  }

  @Override
  public TTransaction startLong() throws TException {
    return ConverterUtils.wrap(txManager.startLong());
  }

  @Override
  public TTransaction startShort() throws TException {
    return ConverterUtils.wrap(txManager.startShort());
  }

  @Override
  public TTransaction startShortTimeout(int timeout) throws TException {
    return ConverterUtils.wrap(txManager.startShort(timeout));
  }


  @Override
  public TBoolean canCommitTx(TTransaction tx, Set<ByteBuffer> changes)
    throws TTransactionNotInProgressException, TException {

    Set<byte[]> changeIds = Sets.newHashSet();
    for (ByteBuffer bb : changes) {
      byte[] changeId = new byte[bb.remaining()];
      bb.get(changeId);
      changeIds.add(changeId);
    }
    try {
      return new TBoolean(txManager.canCommit(ConverterUtils.unwrap(tx), changeIds));
    } catch (TransactionNotInProgressException e) {
      throw new TTransactionNotInProgressException(e.getMessage());
    }
  }

  @Override
  public TBoolean commitTx(TTransaction tx) throws TTransactionNotInProgressException, TException {
    try {
      return new TBoolean(txManager.commit(ConverterUtils.unwrap(tx)));
    } catch (TransactionNotInProgressException e) {
      throw new TTransactionNotInProgressException(e.getMessage());
    }
  }

  @Override
  public void abortTx(TTransaction tx) throws TException {
    txManager.abort(ConverterUtils.unwrap(tx));
  }

  @Override
  public boolean invalidateTx(long tx) throws TException {
    return txManager.invalidate(tx);
  }

  @Override
  public void init() throws Exception {
    txManager.startAndWait();
  }

  @Override
  public void destroy() throws Exception {
    txManager.stopAndWait();
  }

  @Override
  public ByteBuffer getSnapshot() throws TTransactionCouldNotTakeSnapshotException, TException {
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
    return txManager.isRunning() ? Constants.Monitor.STATUS_OK : Constants.Monitor.STATUS_NOTOK;
  }
}
