/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.securestore.gcp.cloudkms;

import io.cdap.cdap.securestore.spi.secret.Decoder;
import io.cdap.cdap.securestore.spi.secret.Encoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * SecretInfo Encoder and Decoder.
 */
public class SecretInfoCodec implements Encoder<SecretInfo>, Decoder<SecretInfo> {

  @Override
  public SecretInfo decode(byte[] data) throws IOException {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
      String name = dis.readUTF();
      boolean descriptionExists = dis.readBoolean();
      String description = descriptionExists ? dis.readUTF() : null;
      long creationTimeMs = dis.readLong();

      Map<String, String> properties = new HashMap<>();
      int len = dis.readInt();
      for (int i = 0; i < len; i++) {
        properties.put(dis.readUTF(), dis.readUTF());
      }

      byte[] secret = new byte[dis.readInt()];
      dis.readFully(secret);
      return new SecretInfo(name, description, secret, creationTimeMs, properties);
    }
  }

  @Override
  public byte[] encode(SecretInfo secretInfo) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(bos)) {
      dos.writeUTF(secretInfo.getName());
      dos.writeBoolean(secretInfo.getDescription() != null);
      if (secretInfo.getDescription() != null) {
        dos.writeUTF(secretInfo.getDescription());
      }
      dos.writeLong(secretInfo.getCreationTimeMs());

      Map<String, String> properties = secretInfo.getProperties();
      dos.writeInt(properties.size());
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        dos.writeUTF(entry.getKey());
        dos.writeUTF(entry.getValue());
      }

      byte[] secret = secretInfo.getSecretData();
      dos.writeInt(secret.length);
      dos.write(secret);
    }
    return bos.toByteArray();
  }
}
