package com.continuuity.data.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

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
   * Manages {@code TupleMetaDataAnnotator} during {@code EnqueuePayload} of a
   * tuple.
   */
  public static class EnqueuePayload {

    /**
     * Stores the ids of operation to tuple field mappings.
     */
    private final Map<String, Long> operationIds;

    /**
     * Serialized Tuple
     */
    private byte[] serializedTuple;

    public EnqueuePayload() {
      operationIds = Maps.newHashMap();
      serializedTuple = null;
    }

    public EnqueuePayload(Map<String, Long> operationIds,
                          byte[] serializedTuple) {
      this.operationIds = operationIds;
      this.serializedTuple = serializedTuple;
    }

    public void write(DataOutput out) throws IOException {
      out.writeShort(operationIds.size());
      for(Map.Entry<String, Long> entry : operationIds.entrySet()) {
        out.writeShort(entry.getKey().length());
        out.write(entry.getKey().getBytes());
        out.writeLong(entry.getValue());
      }
      out.writeInt(serializedTuple.length);
      out.write(serializedTuple);
    }

    public void readFields(DataInput in) throws IOException {
      int size = in.readShort();
      for(int i = 0; i < size; ++i) {
        int len = in.readShort();
        byte[] b = new byte[len];
        in.readFully(b, 0, len);
        long val = in.readLong();
        operationIds.put(new String(b), val);
      }
      int len = in.readInt();
      serializedTuple = new byte[len];
      in.readFully(serializedTuple, 0, len);
    }

    /**
     * @return Map of string to location of operation in global list.
     */
    public Map<String, Long> getOperationIds() {
      return operationIds;
    }

    /**
     * @return array of bytes containing the actual tuple.
     */
    public byte[] getSerializedTuple() {
      return serializedTuple;
    }

    /**
     * Helper static function for reading array of bytes and
     * returing an EnqueuePayload object.
     *
     * @param b array of bytes.
     * @return instance of EnqueuePayload object.
     * @throws IOException
     */
    public static EnqueuePayload read(byte[] b) throws IOException {
      EnqueuePayload w = new EnqueuePayload();
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
    public static byte[] write(Map<String, Long> locations,
                               byte[] serializedTuple) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      EnqueuePayload enqueuePayload = new EnqueuePayload(locations, serializedTuple);
      enqueuePayload.write(dos);
      return bos.toByteArray();
    }
  }

  /**
   * Manages {@code TupleMetaDataAnnotator} during {@code DequeuePayload} of a
   * tuple.
   */
  public static class DequeuePayload {
    /**
     * Mapping of field name to the result of execution an operation.
     */
    private final Map<String, Long> values;

    /**
     * Serialized tuple
     */
    private byte[] serializedTuple;

    public DequeuePayload() {
      values = Maps.newHashMap();
      serializedTuple = null;
    }

    public DequeuePayload(Map<String, Long> values,
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
     * Helper static function to create a DequeuePayload instance from
     * the bytes.
     *
     * @param b array of bytes to be serialized.
     * @return instance of DequeuePayload
     * @throws IOException
     */
    public static DequeuePayload read(byte[] b) throws IOException {
      DequeuePayload w = new DequeuePayload();
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
     * @return instance of DequeuePayload.
     * @throws IOException
     */
    public static byte[] write(Map<String, Long> values,
                               byte[] serializedTuple) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      DequeuePayload enqueue = new DequeuePayload(values, serializedTuple);
      enqueue.write(dos);
      return bos.toByteArray();
    }
  }

}
