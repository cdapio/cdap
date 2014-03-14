package com.continuuity.data2.transaction.persist;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.Decoder;
import com.google.common.util.concurrent.AbstractIdleService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public abstract class AbstractTransactionStateStorage extends AbstractIdleService implements TransactionStateStorage {

  protected TransactionSnapshot decode(byte[] bytes) throws IOException {
    return decodeInternal(bytes);
  }

  protected TransactionSnapshot decode(InputStream in) throws IOException {
    return decodeInternal(in);
  }

  protected byte[] encode(TransactionSnapshot snapshot) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      encode(bos, snapshot);
      return bos.toByteArray();
    } finally {
      bos.close();
    }

  }

  protected void encode(OutputStream out, TransactionSnapshot snapshot) {
    SnapshotCodecV2 codec = new SnapshotCodecV2();
    codec.encodeState(out, snapshot);
  }

  // package-private for access in unit-test
  static TransactionSnapshot decodeInternal(byte[] bytes) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    try {
      return decodeInternal(bis);
    } finally {
      bis.close();
    }
  }

  // package-private for access in unit-test
  static TransactionSnapshot decodeInternal(InputStream in) throws IOException {
    AbstractSnapshotCodec codec;
    // Picking at version to create appropriate codec
    Decoder decoder = new BinaryDecoder(in);
    int persistedVersion = decoder.readInt();
    if (SnapshotCodecV1.VERSION == persistedVersion) {
      codec = new SnapshotCodecV1();
    } else if (SnapshotCodecV2.VERSION == persistedVersion) {
      codec = new SnapshotCodecV2();
    } else {
      throw new RuntimeException("Can't decode state persisted with version " + persistedVersion);
    }

    return codec.decodePartialState(in);
  }

}
