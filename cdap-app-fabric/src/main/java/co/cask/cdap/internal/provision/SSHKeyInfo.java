/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.provision;

import java.net.URI;

/**
 * A container for key pair information.
 */
public final class SSHKeyInfo {

  private final URI keyDirectory;
  private final String publicKeyFile;
  private final String privateKeyFile;
  private final String publicKey;
  private final byte[] privateKey;
  private final String username;

  SSHKeyInfo(URI keyDirectory, String publicKeyFile, String privateKeyFile,
             String publicKey, byte[] privateKey, String username) {
    this.keyDirectory = keyDirectory;
    this.publicKeyFile = publicKeyFile;
    this.privateKeyFile = privateKeyFile;
    this.publicKey = publicKey;
    this.privateKey = privateKey;
    this.username = username;
  }

  public URI getKeyDirectory() {
    return keyDirectory;
  }

  public String getPublicKeyFile() {
    return publicKeyFile;
  }

  public String getPrivateKeyFile() {
    return privateKeyFile;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public byte[] getPrivateKey() {
    return privateKey;
  }

  public String getUsername() {
    return username;
  }
}
