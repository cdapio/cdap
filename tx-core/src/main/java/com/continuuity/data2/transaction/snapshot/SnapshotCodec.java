package com.continuuity.data2.transaction.snapshot;

import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Interface to decode and encode a transaction snapshot. Each codec implements one version of the encoding.
 * It need not include the version when encoding the snapshot.
 */
public interface SnapshotCodec {

  /**
   * @return the version of the encoding implemented by the codec.
   */
  int getVersion();

  /**
   * Encode a transaction snapshot into an output stream.
   * @param out the output stream to write to
   * @param snapshot the snapshot to encode
   */
  void encode(OutputStream out, TransactionSnapshot snapshot);

  /**
   * Decode a transaction snapshot from an input stream.
   * @param in the input stream to read from
   * @return the decoded snapshot
   */
  TransactionSnapshot decode(InputStream in);

}
