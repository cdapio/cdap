package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * This serializes and deserializes queue entries.
 */
public final class QueueEntrySerializer {
  private static final Logger LOG = LoggerFactory.getLogger(QueueEntrySerializer.class);

  /**
   * Creates a new serializer. This serializer can be reused.
   */
  public QueueEntrySerializer() {
  }

  /**
   * Serialize a queue entry
   * @param entry the queue entry to be serialized
   * @return the serialized queue entry as a byte array
   * @throws IOException if serialization fails
   */
  public static byte[] serialize(QueueEntry entry) throws IOException {
    if (entry == null) {
      return null;
    }
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(bos);
      Map<String, Integer> map = entry.getPartitioningMap();
      if (map==null || map.size()==0)
        encoder.writeInt(0);
      else {
        encoder.writeInt(map.size());
        for(Map.Entry<String, Integer> e: map.entrySet()) {
          encoder.writeString(e.getKey());
          encoder.writeInt(e.getValue());
        }
      }
      byte[] data = entry.getData();
      if (data==null) {
        data = new byte[0];
      }
      encoder.writeBytes(data);
      return bos.toByteArray();
    } catch (IOException e) {
      LOG.error("Failed to serialize queue entry", e);
      throw e;
    }
  }

  /**
   * Deserialize a queue entry
   * @param bytes the serialized representation of the queue entry
   * @return the deserialized queue entry
   * @throws IOException if deserialization fails
   */
  public static QueueEntry deserialize(byte[] bytes) throws IOException {
    if (bytes == null) {
      return null;
    }
    try {
      ByteArrayInputStream bis=new ByteArrayInputStream(bytes);
      Decoder decoder=new BinaryDecoder(bis);
      int headerSize;
      headerSize=decoder.readInt();
      Map<String,Integer> map=null;
      if (headerSize>0) {
        map=Maps.newHashMap();
        for(int i=0; i<headerSize; i++) {
          map.put(decoder.readString(),decoder.readInt());
        }
      }
      byte[] data=Bytes.toBytes(decoder.readBytes());
      QueueEntry queueEntry=new QueueEntryImpl(data);
      if (map!=null) {
        for(Map.Entry<String, Integer> e: map.entrySet()) {
          queueEntry.addPartitioningKey(e.getKey(),e.getValue());
        }
      }
      return queueEntry;
    } catch (IOException e) {
      LOG.error("Failed to deserialize queue entry", e);
      throw e;
    }
  }
}
