package com.continuuity.data2.transaction.persist;

import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;
import com.google.common.util.concurrent.AbstractIdleService;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Common base class for all transaction storage implementations. This implement logic to prefix a snapshot
 * with a version when encoding, and to select the correct codec for decoding based on this version prefix.
 */
public abstract class AbstractTransactionStateStorage extends AbstractIdleService implements TransactionStateStorage {

  protected final SnapshotCodecProvider codecProvider;

  protected AbstractTransactionStateStorage(SnapshotCodecProvider codecProvider) {
    this.codecProvider = codecProvider;
  }

  @Override
  public void writeSnapshot(OutputStream out, TransactionSnapshot snapshot) throws IOException {
    codecProvider.encode(out, snapshot);
  }
}
