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

  private final URI publicKeyURI;
  private final URI privateKeyURI;
  private final byte[] publicKey;
  private final byte[] privateKey;
  private final String username;

  SSHKeyInfo(URI publicKeyURI, URI privateKeyURI, byte[] publicKey, byte[] privateKey, String username) {
    this.publicKeyURI = publicKeyURI;
    this.privateKeyURI = privateKeyURI;
    this.publicKey = publicKey;
    this.privateKey = privateKey;
    this.username = username;
  }

  public URI getPublicKeyURI() {
    return publicKeyURI;
  }

  public URI getPrivateKeyURI() {
    return privateKeyURI;
  }

  public byte[] getPublicKey() {
    return publicKey;
  }

  public byte[] getPrivateKey() {
    return privateKey;
  }

  public String getUsername() {
    return username;
  }
}
