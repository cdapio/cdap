package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.rpc.RPCServiceHandler;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.distributed.thrift.TTransaction;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionServer;
import com.google.common.collect.Sets;
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
  public boolean canCommitTx(TTransaction tx, Set<ByteBuffer> changes) throws TException {
    Set<byte[]> changeIds = Sets.newHashSet();
    for (ByteBuffer bb : changes) {
      changeIds.add(bb.array());
    }
    return txManager.canCommit(ConverterUtils.unwrap(tx), changeIds);
  }

  @Override
  public boolean commitTx(TTransaction tx) throws TException {
    return txManager.commit(ConverterUtils.unwrap(tx));
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
}
