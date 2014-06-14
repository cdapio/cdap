package com.continuuity.data2.transaction.persist;

import com.continuuity.data2.transaction.snapshot.BinaryDecoder;
import com.continuuity.data2.transaction.snapshot.BinaryEncoder;
import com.continuuity.data2.transaction.snapshot.SnapshotCodec;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;
import com.google.common.util.concurrent.AbstractIdleService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Common base class for all transaction storage implementations. This implement logic to prefix a snapshot
 * with a version when encoding, and to select the correct codec for decoding based on this version prefix.
 */
public abstract class AbstractTransactionStateStorage extends AbstractIdleService implements TransactionStateStorage {

  private final SnapshotCodecProvider codecProvider;

  protected AbstractTransactionStateStorage(SnapshotCodecProvider codecProvider) {
    this.codecProvider = codecProvider;
  }

  @Override
  public TransactionSnapshot readSnapshot(InputStream in) throws IOException {
    // Picking at version to create appropriate codec
    BinaryDecoder decoder = new BinaryDecoder(in);
    int persistedVersion = decoder.readInt();
    SnapshotCodec codec = codecProvider.getCodecForVersion(persistedVersion);
    return codec.decode(in);
  }

  @Override
  public void writeSnapshot(OutputStream out, TransactionSnapshot snapshot) throws IOException {
    SnapshotCodec codec = codecProvider.getCurrentCodec();
    new BinaryEncoder(out).writeInt(codec.getVersion());
    codec.encode(out, snapshot);
  }

}
