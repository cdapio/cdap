package com.continuuity.data.util;

import com.google.common.collect.Maps;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Map;

/**
 * TupleMetaDataAnnotator is responsible for annotating a tuple
 * with additional information like operations specified in tuple.
 * <pre>
 *   Tuple tuple = new TupleBuilder().set("counter",
 *                              new Increment(Bytes.toBytes("counter"), 1)
 *                              .create();
 * </pre>
 * <p>
 *  An operation is added to tuple while creating it. The operation passed
 *  in tuple is executed as part of the transaction along with other operations.
 *  During, dequeue, the result of this operation is made available in the
 *  tuple.
 * </p>
 *
 * <p>
 *   This class is responsible for adding and retreiving meta data about
 *   operations.
 * </p>
 */
public class TupleMetaDataAnnotator {

  /**
   * Manages {@code TupleMetaDataAnnotator} during {@link Enqueue} of a
   * tuple.
   */
  public static class Enqueue implements Writable {

    /**
     * Stores the location of tuple fields to the location of
     * operations in global operation list.
     */
    private final Map<String, Short> locations;

    /**
     * Serialized Tuple
     */
    private byte[] serializedTuple;

    public Enqueue() {
      locations = Maps.newHashMap();
      serializedTuple = null;
    }

    public Enqueue(Map<String, Short> locations,
                   byte[] serializedTuple) {
      this.locations = locations;
      this.serializedTuple = serializedTuple;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeShort(locations.size());
      for(Map.Entry<String, Short> entry : locations.entrySet()) {
        out.writeShort(entry.getKey().length());
        out.write(entry.getKey().getBytes());
        out.writeShort(entry.getValue());
      }
      out.writeInt(serializedTuple.length);
      out.write(serializedTuple);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readShort();
      for(int i = 0; i < size; ++i) {
        int len = in.readShort();
        byte[] b = new byte[len];
        in.readFully(b, 0, len);
        short val = in.readShort();
        locations.put(new String(b), val);
      }
      int len = in.readInt();
      serializedTuple = new byte[len];
      in.readFully(serializedTuple, 0, len);
    }

    /**
     * @return Map of string to location of operation in global list.
     */
    public Map<String, Short> getLocations() {
      return locations;
    }

    /**
     * @return array of bytes containing the actual tuple.
     */
    public byte[] getSerializedTuple() {
      return serializedTuple;
    }

    /**
     * Helper static function for reading array of bytes and
     * returing an Enqueue object.
     *
     * @param b array of bytes.
     * @return instance of Enqueue object.
     * @throws IOException
     */
    public static Enqueue read(byte[] b) throws IOException {
      Enqueue w = new Enqueue();
      ByteArrayInputStream bis = new ByteArrayInputStream(b);
      DataInputStream dis = new DataInputStream(bis);
      w.readFields(dis);
      return w;
    }

    /**
     * Helper static function for creating array of bytes from
     * given location map and serialized tuple.
     *
     * @param locations map of field name and location of operations.
     * @param serializedTuple array of bytes containing tuple
     * @return array of bytes serializing the above info.
     * @throws IOException
     */
    public static byte[] write(Map<String, Short> locations,
                               byte[] serializedTuple) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      Enqueue enqueue = new Enqueue(locations, serializedTuple);
      enqueue.write(dos);
      return bos.toByteArray();
    }
  }

  /**
   * Manages {@code TupleMetaDataAnnotator} during {@link Dequeue} of a
   * tuple.
   */
  public static class Dequeue implements Writable {
    /**
     * Mapping of field name to the result of execution an operation.
     */
    private final Map<String, Long> values;

    /**
     * Serialized tuple
     */
    private byte[] serializedTuple;

    public Dequeue() {
      values = Maps.newHashMap();
      serializedTuple = null;
    }

    public Dequeue(Map<String, Long> values,
                   byte[] serializedTuple) {

      this.values = values;
      this.serializedTuple = serializedTuple;
    }

    public Map<String, Long> getValues() {
      return values;
    }

    public byte[] getSerializedTuple() {
      return serializedTuple;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeShort(values.size());
      for(Map.Entry<String, Long> value : values.entrySet()) {
        out.writeShort(value.getKey().length());
        out.write(value.getKey().getBytes());
        out.writeLong(value.getValue());
      }
      out.writeInt(serializedTuple.length);
      out.write(serializedTuple);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int count = in.readShort();
      for(int i = 0; i <  count; ++i) {
        int len = in.readShort();
        byte[] b = new byte[len];
        in.readFully(b, 0, len);
        long val = in.readLong();
        values.put(new String(b), val);
      }
      int len = in.readInt();
      serializedTuple = new byte[len];
      in.readFully(serializedTuple, 0, len);
    }

    /**
     * Helper static function to create a Dequeue instance from
     * the bytes.
     *
     * @param b array of bytes to be serialized.
     * @return instance of Dequeue
     * @throws IOException
     */
    public static Dequeue read(byte[] b) throws IOException {
      Dequeue w = new Dequeue();
      ByteArrayInputStream bis = new ByteArrayInputStream(b);
      DataInputStream dis = new DataInputStream(bis);
      w.readFields(dis);
      return w;
    }

    /**
     * Helper static function to create array of bytes from the
     * map of field name to values and serialized tuple.
     *
     * @param values map of field name to values.
     * @param serializedTuple array of bytes
     * @return instance of Dequeue.
     * @throws IOException
     */
    public static byte[] write(Map<String, Long> values,
                               byte[] serializedTuple) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      Dequeue enqueue = new Dequeue(values, serializedTuple);
      enqueue.write(dos);
      return bos.toByteArray();
    }
  }

}
