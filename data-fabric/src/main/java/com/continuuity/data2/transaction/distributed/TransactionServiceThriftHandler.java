package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.rpc.RPCServiceHandler;
import com.continuuity.data2.transaction.TransactionNotInProgressException;
import com.continuuity.data2.transaction.distributed.thrift.TBoolean;
import com.continuuity.data2.transaction.distributed.thrift.TTransaction;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionCouldNotTakeSnapshotException;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionNotInProgressException;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionServer;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.SnapshotCodecV2;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.common.collect.Sets;
import java.io.IOException;
import org.apache.thrift.TException;

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
  public void invalidateTx(TTransaction tx) throws TException {
    txManager.invalidate(ConverterUtils.unwrap(tx));
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
      TransactionSnapshot snapshot = txManager.getSnapshot();
      if (snapshot == null) {
        throw new TTransactionCouldNotTakeSnapshotException("Transaction manager could not get a snapshot.");
      }
      SnapshotCodecV2 codec = new SnapshotCodecV2();
      // todo find a way to encode directly to the stream, without having the snapshot in memory twice
      byte[] encoded = codec.encodeState(snapshot);
      return ByteBuffer.wrap(encoded);
    } catch (IOException e) {
      throw new TTransactionCouldNotTakeSnapshotException(e.getMessage());
    }
  }
}
