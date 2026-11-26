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

package io.cdap.cdap.runtime.spi.ssh;

import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import java.util.function.Supplier;

/**
 * Represents a public-private key pair for SSH authentication.
 *
 * @deprecated SSH-based runtime execution and monitoring are no longer supported; use
 *     {@link RuntimeJobManager} and {@link RuntimeMonitorType#URL} instead.
 */
@Deprecated
public class SSHKeyPair {

  private final SSHPublicKey publicKey;
  private final Supplier<byte[]> privateKeySupplier;

  public SSHKeyPair(SSHPublicKey publicKey, Supplier<byte[]> privateKeySupplier) {
    this.publicKey = publicKey;
    this.privateKeySupplier = privateKeySupplier;
  }

  /**
   * Returns a {@link SSHPublicKey} that contains the user and public key for SSH.
   */
  public SSHPublicKey getPublicKey() {
    return publicKey;
  }

  /**
   * Returns a {@link Supplier} that will supply the private key for SSH.
   */
  public Supplier<byte[]> getPrivateKeySupplier() {
    return privateKeySupplier;
  }
}
