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

package co.cask.cdap.securestore.gcp.cloudkms;

import co.cask.cdap.securestore.spi.secret.Decoder;
import co.cask.cdap.securestore.spi.secret.Encoder;
import co.cask.cdap.securestore.spi.secret.Secret;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Secret Encoder and Decoder.
 */
public class SecretEncoderDecoder implements Encoder<Secret>, Decoder<Secret> {

  @Override
  public Secret decode(byte[] bytes) throws IOException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInput in = new ObjectInputStream(bis)) {
      return (Secret) in.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Error encountered while decoding the secret", e);
    }
  }

  @Override
  public byte[] encode(Secret secret) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(secret);
      return bos.toByteArray();
    }
  }
}
