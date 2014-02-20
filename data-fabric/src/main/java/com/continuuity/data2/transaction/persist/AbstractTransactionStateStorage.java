package com.continuuity.data2.transaction.persist;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.Decoder;
import com.google.common.util.concurrent.AbstractIdleService;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 *
 */
public abstract class AbstractTransactionStateStorage extends AbstractIdleService implements TransactionStateStorage {

  protected TransactionSnapshot decode(byte[] bytes) throws IOException {
    return decodeInternal(bytes);
  }

  protected byte[] encode(TransactionSnapshot snapshot) {
    SnapshotCodecV2 codec = new SnapshotCodecV2();
    return codec.encodeState(snapshot);
  }

  // package-private for access in unit-test
  static TransactionSnapshot decodeInternal(byte[] bytes) throws IOException {
    AbstractSnapshotCodec codec;
    // Picking at version to create appropriate codec
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    Decoder decoder = new BinaryDecoder(bis);
    int persistedVersion = decoder.readInt();
    if (SnapshotCodecV1.VERSION == persistedVersion) {
      codec = new SnapshotCodecV1();
    } else if (SnapshotCodecV2.VERSION == persistedVersion) {
      codec = new SnapshotCodecV2();
    } else {
      throw new RuntimeException("Can't decode state persisted with version " + persistedVersion);
    }

    return codec.decodeState(bytes);
  }


}
