/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security.store;

import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.Key;

/**
 * An adapter between a KeyStore Key and our entry. This is used to store
 * the data and the metadata in a KeyStore.
 */
class KeyStoreEntry implements Key, Serializable {
  private static final long serialVersionUID = 3405839418917868651L;
  private static final String METADATA_FORMAT = "KeyStoreEntry";
  // Java Keystore needs an algorithm name to store a key, it is not used for any checks, only stored,
  // since we are not handling the encryption we don't care about this.
  private static final String ALGORITHM_PROXY = "none";
  private static final Gson GSON = new Gson();

  private SecureStoreData data;
  private SecureStoreMetadata metadata;

  KeyStoreEntry(SecureStoreData data, SecureStoreMetadata meta) {
    this.data = data;
    this.metadata = meta;
  }

  @Override
  // This method is never called. It is here to satisfy the Key interface. We need to implement the key interface
  // so that we can store our entry in the keystore.
  public String getAlgorithm() {
    return ALGORITHM_PROXY;
  }

  @Override
  // This method is never called. It is here to satisfy the Key interface. We need to implement the key interface
  // so that we can store our entry in the keystore.
  public String getFormat() {
    return METADATA_FORMAT;
  }

  @Override
  // This method is never called. It is here to satisfy the Key interface. We need to implement the key interface
  // so that we can store our entry in the keystore.
  public byte[] getEncoded() {
    return new byte[0];
  }

  /**
   * Serialize the metadata to a byte array.
   *
   * @return the serialized bytes
   */
  private static byte[] serializeMetadata(SecureStoreMetadata metadata) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    OutputStreamWriter out = new OutputStreamWriter(buffer, Charset.forName("UTF-8"));
    KeyStoreEntry.GSON.toJson(metadata, out);
    out.close();
    return buffer.toByteArray();
  }

  /**
   * Deserialize a new metadata object from a byte array.
   *
   * @param buf the serialized metadata
   */
  private static SecureStoreMetadata deserializeMetadata(byte[] buf) throws IOException {
    try (JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(buf),
                                                                  Charset.forName("UTF-8")))) {
      return KeyStoreEntry.GSON.fromJson(reader, SecureStoreMetadata.class);
    }
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    byte[] serializedMetadata = serializeMetadata(metadata);
    byte[] binaryData = data.get();

    out.writeInt(serializedMetadata.length);
    out.write(serializedMetadata);
    out.writeInt(binaryData.length);
    out.write(binaryData);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    byte[] buf = new byte[in.readInt()];
    in.readFully(buf);
    byte[] dataBuf = new byte[in.readInt()];
    in.readFully(dataBuf);
    metadata = deserializeMetadata(buf);
    data = new SecureStoreData(metadata, dataBuf);
  }

  SecureStoreData getData() {
    return data;
  }

  SecureStoreMetadata getMetadata() {
    return metadata;
  }
}
