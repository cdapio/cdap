/*
 * Copyright 2012-2014 Continuuity, Inc.
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
