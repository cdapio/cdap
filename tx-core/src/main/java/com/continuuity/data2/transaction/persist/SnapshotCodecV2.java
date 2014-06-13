package com.continuuity.data2.transaction.persist;

import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Handles serialization/deserialization of a {@link TransactionSnapshot} and its elements to {@code byte[]}.
 */
public class SnapshotCodecV2 extends AbstractSnapshotCodec {
  public static final int VERSION = 2;

  @Override
  protected int getVersion() {
    return VERSION;
  }

  @Override
  protected void encodeInProgress(Encoder encoder, Map<Long, InMemoryTransactionManager.InProgressTx> inProgress)
    throws IOException {

    if (!inProgress.isEmpty()) {
      encoder.writeInt(inProgress.size());
      for (Map.Entry<Long, InMemoryTransactionManager.InProgressTx> entry : inProgress.entrySet()) {
        encoder.writeLong(entry.getKey()); // tx id
        encoder.writeLong(entry.getValue().getExpiration());
        encoder.writeLong(entry.getValue().getVisibilityUpperBound());
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  @Override
  protected NavigableMap<Long, InMemoryTransactionManager.InProgressTx> decodeInProgress(Decoder decoder)
    throws IOException {

    int size = decoder.readInt();
    NavigableMap<Long, InMemoryTransactionManager.InProgressTx> inProgress = Maps.newTreeMap();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        long txId = decoder.readLong();
        long expiration = decoder.readLong();
        long visibilityUpperBound = decoder.readLong();
        inProgress.put(txId,
                       new InMemoryTransactionManager.InProgressTx(visibilityUpperBound, expiration));
      }
      size = decoder.readInt();
    }
    return inProgress;
  }
}
