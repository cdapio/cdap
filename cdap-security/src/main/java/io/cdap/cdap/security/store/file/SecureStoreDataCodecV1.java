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
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Version 1 codec for {@link SecureStoreData} and providing the {@link java.security.KeyStore}
 * scheme.
 *
 * Version 1 codec uses JCEKS KeyStore and directly converts the namespace and key name to a key
 * alias in
 * <namespace>-<key> format. When encoding and decoding a {@link SecureStoreData} struct, each
 * piece
 * of metadata along with its length is written in binary format. See {@link
 * #encode(SecureStoreData)} and {@link #decode(byte[])} for details.
 *
 * NOTE: This codec should not be used in normal operation and remains here for backwards
 * compatibility purposes. See {@link SecureStoreDataCodecV2} instead.
 */
@Deprecated
public class SecureStoreDataCodecV1 implements FileSecureStoreCodec {

  /**
   * Scheme for KeyStore instance.
   */
  private static final String SCHEME_JCEKS = "jceks";
  /**
   * Separator between key namespace and key name in key alias.
   */
  private static final String NAME_SEPARATOR = ":";

  @Override
  public String getKeyStoreScheme() {
    return SCHEME_JCEKS;
  }

  @Override
  public String getKeyAliasFromInfo(KeyInfo keyInfo) {
    return keyInfo.getNamespace() + NAME_SEPARATOR + keyInfo.getName();
  }

  @Override
  public KeyInfo getKeyInfoFromAlias(String keyAlias) {
    String[] namespaceAndName = keyAlias.split(NAME_SEPARATOR);
    Preconditions.checkArgument(namespaceAndName.length == 2);
    return new KeyInfo(namespaceAndName[0], namespaceAndName[1]);
  }

  @Override
  public String getAliasSearchPrefix(String namespace) {
    return namespace + NAME_SEPARATOR;
  }

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

    return bos.toByteArray();
  }

  @Override
  public SecureStoreData decode(byte[] data) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

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

    return new SecureStoreData(meta, secret);
  }
}
