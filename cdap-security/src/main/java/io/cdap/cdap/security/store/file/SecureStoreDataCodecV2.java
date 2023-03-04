/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.security.store.file;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Version 2 codec for {@link SecureStoreData} and providing the {@link java.security.KeyStore}
 * scheme.
 *
 * Version 2 codec uses PKCS12 KeyStore and directly converts the hex-encoded namespace and key name
 * to a key alias in <hex-encoded-namespace>-<key> format. When encoding and decoding a {@link
 * SecureStoreData} struct, each piece of metadata along with its length is written in binary
 * format. See {@link #encode(SecureStoreData)} and {@link #decode(byte[])} for details.
 */
public class SecureStoreDataCodecV2 implements FileSecureStoreCodec {

  /**
   * Scheme for KeyStore instance.
   */
  private static final String SCHEME_PKCS12 = "PKCS12";
  /**
   * Separator between key namespace and key name in key alias.
   */
  private static final String NAME_SEPARATOR = ":";

  @Override
  public String getKeyStoreScheme() {
    return SCHEME_PKCS12;
  }

  @Override
  public String getKeyAliasFromInfo(KeyInfo keyInfo) {
    // Namespaces can be case-sensitive, while key names cannot. As we cannot assume the backing implementation of
    // KeyStore supports case-sensitive aliases, the namespace must be hex-encoded during alias generation.
    return Bytes.toHexString(keyInfo.getNamespace().getBytes(StandardCharsets.UTF_8))
        + NAME_SEPARATOR
        + keyInfo.getName();
  }

  @Override
  public KeyInfo getKeyInfoFromAlias(String keyAlias) {
    String[] namespaceAndName = keyAlias.split(NAME_SEPARATOR);
    Preconditions.checkArgument(namespaceAndName.length == 2);
    return new KeyInfo(namespaceAndName[0], namespaceAndName[1]);
  }

  @Override
  public String getAliasSearchPrefix(String namespace) {
    // Namespaces can be case-sensitive, while key names cannot. As we cannot assume the backing implementation of
    // KeyStore supports case-sensitive aliases, the namespace must be hex-encoded during prefix generation.
    return Bytes.toHexString(namespace.getBytes(StandardCharsets.UTF_8)) + NAME_SEPARATOR;
  }

  /**
   * Serializes a {@link SecureStoreData} object to a base64-encoded character array.
   *
   * NOTICE: IF YOU ARE MAKING A BACKWARDS INCOMPATIBLE CHANGE, YOU MUST CREATE A NEW CODEC.
   *
   * @param data The data to serialize
   * @return Base64-encoded byte array.
   */
  @Override
  public byte[] encode(SecureStoreData data) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(bos)) {
      // Writing the metadata
      SecureStoreMetadata meta = data.getMetadata();
      dos.writeUTF(meta.getName());
      dos.writeBoolean(meta.getDescription() != null);
      if (meta.getDescription() != null) {
        dos.writeUTF(meta.getDescription());
      }
      dos.writeLong(meta.getLastModifiedTime());

      Map<String, String> properties = meta.getProperties();
      dos.writeInt(properties.size());
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        dos.writeUTF(entry.getKey());
        dos.writeUTF(entry.getValue());
      }

      byte[] secret = data.get();
      dos.writeInt(secret.length);
      dos.write(secret);
    }

    return Base64.getEncoder().encode(bos.toByteArray());
  }

  /**
   * Deserializes a base64-encoded character array to a {@link SecureStoreData} object.
   *
   * @param data The data to deserialize
   * @return Base64-encoded byte array.
   */
  @Override
  public SecureStoreData decode(byte[] data) throws IOException {
    byte[] decodedData = Base64.getDecoder().decode(data);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(decodedData));
    Arrays.fill(data, (byte) 0x0);

    String name = dis.readUTF();
    boolean descriptionExists = dis.readBoolean();
    String description = descriptionExists ? dis.readUTF() : null;
    long lastModified = dis.readLong();
    Map<String, String> properties = new HashMap<>();
    int len = dis.readInt();
    for (int i = 0; i < len; i++) {
      properties.put(dis.readUTF(), dis.readUTF());
    }
    SecureStoreMetadata meta = new SecureStoreMetadata(name, description, lastModified, properties);
    byte[] secret = new byte[dis.readInt()];
    dis.readFully(secret);

    Arrays.fill(decodedData, (byte) 0x0);

    return new SecureStoreData(meta, secret);
  }
}
