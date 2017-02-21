/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.snapshot;

import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.persist.TransactionSnapshot;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Handles serialization/deserialization of a {@link TransactionSnapshot}
 * and its elements to {@code byte[]}.
 */
public class SnapshotCodecV2 extends DefaultSnapshotCodec {
  @Override
  public int getVersion() {
    return 2;
  }

  @Override
  protected void encodeInProgress(BinaryEncoder encoder, Map<Long, TransactionManager.InProgressTx> inProgress)
    throws IOException {

    if (!inProgress.isEmpty()) {
      encoder.writeInt(inProgress.size());
      for (Map.Entry<Long, TransactionManager.InProgressTx> entry : inProgress.entrySet()) {
        encoder.writeLong(entry.getKey()); // tx id
        encoder.writeLong(entry.getValue().getExpiration());
        encoder.writeLong(entry.getValue().getVisibilityUpperBound());
        encoder.writeInt(entry.getValue().getType().ordinal());
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  @Override
  protected NavigableMap<Long, TransactionManager.InProgressTx> decodeInProgress(BinaryDecoder decoder)
    throws IOException {

    int size = decoder.readInt();
    NavigableMap<Long, TransactionManager.InProgressTx> inProgress = Maps.newTreeMap();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        long txId = decoder.readLong();
        long expiration = decoder.readLong();
        long visibilityUpperBound = decoder.readLong();
        int txTypeIdx = decoder.readInt();
        TransactionManager.InProgressType txType;
        try {
          txType = TransactionManager.InProgressType.values()[txTypeIdx];
        } catch (ArrayIndexOutOfBoundsException e) {
          throw new IOException("Type enum ordinal value is out of range: " + txTypeIdx);
        }
        inProgress.put(txId,
                       new TransactionManager.InProgressTx(visibilityUpperBound, expiration, txType,
                           new LongArrayList()));
      }
      size = decoder.readInt();
    }
    return inProgress;
  }
}
