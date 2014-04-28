package com.continuuity.data2.transaction.inmemory;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionCouldNotTakeSnapshotException;
import com.continuuity.data2.transaction.TransactionNotInProgressException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionCouldNotTakeSnapshotException;
import com.continuuity.data2.transaction.persist.SnapshotCodecV2;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class InMemoryTxSystemClient implements TransactionSystemClient {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTxSystemClient.class);

  InMemoryTransactionManager txManager;

  @Inject
  public InMemoryTxSystemClient(InMemoryTransactionManager txmgr) {
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
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    try {
      SnapshotCodecV2 codec = new SnapshotCodecV2();
      TransactionSnapshot snapshot = txManager.getSnapshot();
      if (snapshot == null) {
        throw new TransactionCouldNotTakeSnapshotException("Transaction manager could not get a snapshot.");
      }
      // todo find a way to encode directly to the stream, without having the snapshot in memory twice
      byte[] encoded = codec.encodeState(snapshot);
      return new ByteArrayInputStream(encoded);
    } catch (IOException e) {
      LOG.error("Snapshot could not be taken", e);
      throw new TransactionCouldNotTakeSnapshotException(e.getMessage());
    }
  }

  @Override
  public void resetState() {
    txManager.resetState();
  }
}
