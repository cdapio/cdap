package com.continuuity.data2.transaction.snapshot;

import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Handles serialization/deserialization of a {@link com.continuuity.data2.transaction.persist.TransactionSnapshot} and
 * its elements to {@code byte[]}.
 */
public class SnapshotCodecV1 extends AbstractSnapshotCodec {
  public static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  protected void decodeObsoleteAttributes(BinaryDecoder decoder) throws IOException {
    // watermark attribute was removed
    decoder.readLong();
  }

  @Override
  protected void encodeObsoleteAttributes(BinaryEncoder encoder) throws IOException {
    // writing watermark attribute (that was removed in newer codecs), 55L - any random value, will not be used anywhere
    encoder.writeLong(55L);
  }

  @Override
  protected void encodeInProgress(BinaryEncoder encoder, Map<Long, InMemoryTransactionManager.InProgressTx> inProgress)
    throws IOException {

    if (!inProgress.isEmpty()) {
      encoder.writeInt(inProgress.size());
      for (Map.Entry<Long, InMemoryTransactionManager.InProgressTx> entry : inProgress.entrySet()) {
        encoder.writeLong(entry.getKey()); // tx id
        encoder.writeLong(entry.getValue().getExpiration());
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  @Override
  protected NavigableMap<Long, InMemoryTransactionManager.InProgressTx> decodeInProgress(BinaryDecoder decoder)
    throws IOException {

    int size = decoder.readInt();
    NavigableMap<Long, InMemoryTransactionManager.InProgressTx> inProgress = Maps.newTreeMap();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        inProgress.put(decoder.readLong(),
                       // 1st version did not store visibilityUpperBound. It is safe to set firstInProgress to 0,
                       // it may decrease performance until this tx is finished, but correctness will be preserved.
                       new InMemoryTransactionManager.InProgressTx(0L, decoder.readLong()));
      }
      size = decoder.readInt();
    }
    return inProgress;
  }
}
