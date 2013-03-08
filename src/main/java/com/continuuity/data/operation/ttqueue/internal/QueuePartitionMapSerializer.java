package com.continuuity.data.operation.ttqueue.internal;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueueEntryImpl;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * This serializes and deserializes queue partition maps.
 */
public final class QueuePartitionMapSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(QueuePartitionMapSerializer.class);
  private static final int version = 0;

  /**
   * Creates a new serializer. This serializer can be reused.
   */
  public QueuePartitionMapSerializer() {
  }

  /**
   * Serialize a queue entry
   *
   * @param map the partition map to be serialized
   * @return the serialized partition map as a byte array
   * @throws java.io.IOException if serialization fails
   */
  public static byte[] serialize(Map<String, Integer> map) throws IOException {
    if (map == null) {
      return null;
    }
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(bos);

      //writing version number which might be needed in the future if we change the schema
      encoder.writeInt(version);
      if (map == null || map.size() == 0) encoder.writeInt(0);
      else {
        encoder.writeInt(map.size());
        for (Map.Entry<String, Integer> e : map.entrySet()) {
          encoder.writeString(e.getKey());
          encoder.writeInt(e.getValue());
        }
      }
      return bos.toByteArray();
    } catch (IOException e) {
      LOG.error("Failed to serialize partition map", e);
      throw e;
    }
  }

  /**
   * Deserialize a partition map
   *
   * @param bytes the serialized representation of the partition map
   * @return the deserialized partition map
   * @throws java.io.IOException if deserialization fails
   */
  public static Map<String, Integer> deserialize(byte[] bytes) throws IOException {
    if (bytes == null) {
      return null;
    }
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      Decoder decoder = new BinaryDecoder(bis);

      //Making sure versions of code and queue entry data are compatible
      int versionFound = decoder.readInt();
      if (versionFound != version) {
        throw new IOException("Version of queue entry data incompatible!");
      }
      int headerSize = decoder.readInt();
      Map<String, Integer> map = null;
      if (headerSize > 0) {
        map = Maps.newHashMap();
        for (int i = 0; i < headerSize; i++) {
          map.put(decoder.readString(), decoder.readInt());
        }
      }
      return map;
    } catch (IOException e) {
      LOG.error("Failed to deserialize partition map", e);
      throw e;
    }
  }
}
