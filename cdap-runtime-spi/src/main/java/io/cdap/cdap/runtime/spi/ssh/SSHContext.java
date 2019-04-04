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

import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;

import java.io.IOException;
import java.security.KeyException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * This context provides ssh keys and access to {@link SSHSession} for interacting with remote hosts.
 */
public interface SSHContext {

  /**
   * Generates a 2048 bits RSA secure key pair for SSH operations.
   *
   * @param user the user name used for SSH.
   * @return a {@link SSHKeyPair}
   * @throws KeyException if failed to generate the key pair
   */
  default SSHKeyPair generate(String user) throws KeyException {
    return generate(user, 2048);
  }

  /**
   * Generates a RSA secure key pair for SSH operations.
   *
   * @param user the user name used for SSH.
   * @param bits number of bits in the RSA key. The longer it is, the more secure.
   * @return a {@link SSHKeyPair}
   * @throws KeyException if failed to generate the key pair
   */
  SSHKeyPair generate(String user, int bits) throws KeyException;

  /**
   * Sets the SSH key pair for the platform to communicate with the cluster in future.
   * This method can only be called during the {@link Provisioner#createCluster(ProvisionerContext)} call.
   *
   * @param sshKeyPair the {@link SSHKeyPair} to use
   */
  void setSSHKeyPair(SSHKeyPair sshKeyPair);

  /**
   * Returns the {@link SSHKeyPair} that were set earlier via the {@link #setSSHKeyPair(SSHKeyPair)} method during the
   * {@link Provisioner#createCluster(ProvisionerContext)} time.
   *
   * @return an {@link Optional} of {@link SSHKeyPair}
   */
  Optional<SSHKeyPair> getSSHKeyPair();

  /**
   * Creates a {@link SSHSession} to the given host. It uses the {@link SSHKeyPair}
   * set via the {@link #setSSHKeyPair(SSHKeyPair)} method.
   *
   * @param host hostname to ssh to
   * @return a new {@link SSHSession}
   * @throws IOException if failed to create a new session to the host
   */
  default SSHSession createSSHSession(String host) throws IOException {
    return createSSHSession(getSSHKeyPair().orElseThrow(() -> new IllegalStateException("No SSHKeyPair available")),
                            host);
  }

  /**
   * Creates a {@link SSHSession} to the given host.
   *
   * @param host hostname to ssh to
   * @return a new {@link SSHSession}
   * @throws IOException if failed to create a new session to the host
   */
  default SSHSession createSSHSession(SSHKeyPair keyPair, String host) throws IOException {
    return createSSHSession(keyPair.getPublicKey().getUser(), keyPair.getPrivateKeySupplier(),
                            host, 22, Collections.emptyMap());
  }

  /**
   * Creates a {@link SSHSession} to the given host, on a specific port, and with extra sets of ssh configurations.
   *
   * @param user user name for ssh
   * @param privateKeySupplier a {@link Supplier} to private key used for ssh
   * @param host hostname to ssh to
   * @param port the port to connect to
   * @param configs set of extra configurations
   * @return a new {@link SSHSession}
   * @throws IOException if failed to create a new session to the host
   */
  SSHSession createSSHSession(String user, Supplier<byte[]> privateKeySupplier, String host,
                              int port, Map<String, String> configs) throws IOException;
}
