package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Encoder;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class QueueEntry {
  private final Map<String, Integer> hashKeys;
  private byte[] data;

  public QueueEntry(Map<String, Integer> hashKeys, byte[] data) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(hashKeys);
    this.data = data;
    this.hashKeys = hashKeys;
  }

  public QueueEntry(byte[] data) {
    Preconditions.checkNotNull(data);
    this.hashKeys = Maps.newHashMap();
    this.data = data;
  }

  public byte[] getData() {
    return this.data;
  }

  public Map<String, Integer> getHashKeys() {
    return hashKeys;
  }

  public void addHashKey(String key, int hash) {
    this.hashKeys.put(key, hash);
  }

  public Integer getHashKey(String key) {
    if (hashKeys == null) {
      return null;
    }
    return this.hashKeys.get(key);
  }

  public String toString() {
    return Objects.toStringHelper(this)
      .add("data", this.data)
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
  private final static byte[] serializedEmptyHashKeys = serializeEmptyHashKeys();

  public static byte[] serializeHashKeys(Map<String, Integer> hashKeys) throws IOException {
    // many entries will have no hash keys. Reuse a static value for that
    if (hashKeys == null || hashKeys.isEmpty()) {
      return serializedEmptyHashKeys;
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);
    encoder.writeInt(hashKeys.size());
    for (Map.Entry<String, Integer> entry : hashKeys.entrySet()) {
      encoder.writeString(entry.getKey());
      encoder.writeInt(entry.getValue());
    }
    encoder.writeInt(0); // per Avro spec, end with a (block of length) zero
    return bos.toByteArray();
  }

  public static Map<String, Integer> deserializeHashKeys(byte[] bytes) throws IOException {
    if (bytes == null) {
      return null;
    }
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
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