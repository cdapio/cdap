/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.data2.queue;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.cdap.common.io.Encoder;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Defines QueueEntry.
 */
public class QueueEntry {
  private final Map<String, Integer> hashKeys;
  private final byte[] data;

  public QueueEntry(byte[] data) {
    this(ImmutableMap.<String, Integer>of(), data);
  }

  public QueueEntry(String hashKey, int hashValue, byte[] data) {
    this(ImmutableMap.of(hashKey, hashValue), data);
  }

  public QueueEntry(Map<String, Integer> hashKeys, byte[] data) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(hashKeys);
    this.data = data;
    this.hashKeys = ImmutableMap.copyOf(hashKeys);
  }

  public byte[] getData() {
    return this.data;
  }

  public Map<String, Integer> getHashKeys() {
    return hashKeys;
  }

  public Integer getHashKey(String key) {
    return this.hashKeys.get(key);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("data", Bytes.toStringBinary(this.data))
      .add("hashKeys", this.hashKeys)
      .toString();
  }

  // many entries will have no hash keys. Serialize that once and for good
  private static byte[] serializeEmptyHashKeys() {
    try {
      // we don't synchronize here: the worst thing that go wrong here is repeated assignment to the same value
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(bos);
      encoder.writeInt(0);
      return bos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("encoding empty hash keys went wrong - bailing out: " + e.getMessage(), e);
    }
  }
  private static final byte[] SERIALIZED_EMPTY_HASH_KEYS = serializeEmptyHashKeys();

  public static byte[] serializeHashKeys(Map<String, Integer> hashKeys) throws IOException {
    // many entries will have no hash keys. Reuse a static value for that
    if (hashKeys == null || hashKeys.isEmpty()) {
      return SERIALIZED_EMPTY_HASH_KEYS;
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);
    encoder.writeInt(hashKeys.size());
    for (Map.Entry<String, Integer> entry : hashKeys.entrySet()) {
      encoder.writeString(entry.getKey()).writeInt(entry.getValue());
    }
    encoder.writeInt(0); // per Avro spec, end with a (block of length) zero
    return bos.toByteArray();
  }

  public static Map<String, Integer> deserializeHashKeys(byte[] bytes) throws IOException {
    return deserializeHashKeys(bytes, 0, bytes.length);
  }

  public static Map<String, Integer> deserializeHashKeys(byte[] bytes, int off, int len) throws IOException {
    if (bytes == null || (len == 1 && bytes[off] == 0)) {
      // No hash keys.
      return ImmutableMap.of();
    }
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes, off, len);
    BinaryDecoder decoder = new BinaryDecoder(bis);
    int size = decoder.readInt();
    Map<String, Integer> hashKeys = Maps.newHashMapWithExpectedSize(size);
    while (size > 0) { // per avro spec, ther ecan be multiple blocks
      while (size-- > 0) {
        String key = decoder.readString();
        int value = decoder.readInt();
        hashKeys.put(key, value);
      }
      size = decoder.readInt(); // next block length, will be always zero in this case
    }
    return hashKeys;
  }
}
