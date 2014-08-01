/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.data2.transaction.snapshot;

import com.continuuity.tephra.inmemory.InMemoryTransactionManager;
import com.continuuity.tephra.snapshot.BinaryDecoder;
import com.continuuity.tephra.snapshot.BinaryEncoder;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Handles serialization/deserialization of a {@link com.continuuity.tephra.persist.TransactionSnapshot} and
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
