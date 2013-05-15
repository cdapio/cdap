/**
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.data.metadata;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.Constants;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.TreeMap;

/**
 * This serializes and deserializes meta data entries using Kryo.
 */
public final class MetaDataSerializer {
  private static final Logger LOG =
    LoggerFactory.getLogger(MetaDataSerializer.class);

  private Gson gson ;

  /**
   * Creates a new serializer. This serializer can be reused.
   */
  public MetaDataSerializer() {
    this.gson = new Gson();
  }

  /**
   * Serialize a meta data entry.
   * @param meta the meta data to be serialized
   * @return the serialized meta data as a byte array
   * @throws MetaDataException if serialization fails
   */
  public byte[] serialize(MetaDataEntry meta) throws MetaDataException {
    String s = gson.toJson(meta, meta.getClass());
    return Bytes.toBytes(s);
  }

  /**
   * Deserialize an meta data entry.
   * @param bytes the serialized representation of the meta data
   * @return the deserialized meta data
   * @throws MetaDataException if deserialization fails
   */
  public MetaDataEntry deserialize(byte[] bytes) throws MetaDataException {
    return gson.fromJson(Bytes.toString(bytes), MetaData.class);
  }

  /**
   * This class is needed for Kryo - it requires a default constructor, which MetaDataEntry does not have.
   */
  public static class MetaData extends MetaDataEntry {
    public MetaData() {
      super("-", null, "-", "-");
    }
  }
}
