/**
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.metadata;

import com.continuuity.api.common.Bytes;
import com.google.gson.Gson;

/**
 * This serializes and deserializes meta data entries using Gson.
 * This class is thread-safe.
 */
public final class MetaDataSerializer {

  private ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
    @Override
    protected Gson initialValue() {
      return new Gson();
    }
  };

  /**
   * Creates a new serializer. This serializer can be reused.
   */
  public MetaDataSerializer() {
  }

  /**
   * Serialize a meta data entry.
   * @param meta the meta data to be serialized
   * @return the serialized meta data as a byte array
   */
  public byte[] serialize(MetaDataEntry meta) {
    String s = gson.get().toJson(meta, meta.getClass());
    return Bytes.toBytes(s);
  }

  /**
   * Deserialize an meta data entry.
   * @param bytes the serialized representation of the meta data
   * @return the deserialized meta data
   */
  public MetaDataEntry deserialize(byte[] bytes) {
    return gson.get().fromJson(Bytes.toString(bytes), MetaData.class);
  }

  /**
   * This class is needed for Gson - it requires a default constructor, which MetaDataEntry does not have.
   */
  public static class MetaData extends MetaDataEntry {
    public MetaData() {
      super("-", null, "-", "-");
    }
  }
}
