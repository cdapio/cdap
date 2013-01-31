/**
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.data.metadata;

import com.continuuity.common.conf.Constants;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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

	private final Kryo kryo;
	private final Output output;
  private final Input input;

	/**
	 * Creates a new serializer. This serializer can be reused.
	 */
	public MetaDataSerializer() {
		this.kryo = new Kryo();
    output = new Output(Constants.MAX_SERDE_BUFFER);
    input = new Input(Constants.MAX_SERDE_BUFFER);
		kryo.register(byte[].class);
    kryo.register(TreeMap.class);
    kryo.register(MetaData.class);
    kryo.register(MetaDataEntry.class);
  }

	/**
	 * Serialize a meta data entry
	 * @param meta the meta data to be serialized
	 * @return the serialized meta data as a byte array
   * @throws MetaDataException if serialization fails
	 */
	public byte[] serialize(MetaDataEntry meta) throws MetaDataException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    output.setOutputStream(outStream);
		try {
			kryo.writeObject(output, meta);
      output.flush();
      return outStream.toByteArray();
		} catch (Exception e) {
			LOG.error("Failed to serialize meta data", e);
      throw new MetaDataException("Failed to serialize meta data", e);
		}
	}

	/**
	 * Deserialize an meta data entry
	 * @param bytes the serialized representation of the meta data
	 * @return the deserialized meta data
   * @throws MetaDataException if deserialization fails
   */
	public MetaDataEntry deserialize(byte[] bytes) throws MetaDataException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    input.setInputStream(inputStream);
		try {
      return kryo.readObject(input, MetaData.class);
		} catch (Exception e) {
      LOG.error("Failed to deserialize meta data", e);
      throw new MetaDataException("Failed to deserialize meta data", e);
    }
	}

  public static class MetaData extends MetaDataEntry {
    public MetaData() {
      super("-", null, "-", "-");
    }
  }
}
