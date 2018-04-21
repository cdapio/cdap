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

package co.cask.cdap.runtime.spi.ssh;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * This context provides ssh keys and access to {@link SSHSession} for interacting with remote hosts.
 */
public interface SSHContext {

  /**
   * Returns the {@link SSHPublicKey} generated for this context.
   */
  SSHPublicKey getSSHPublicKey();

  /**
   * Creates a {@link SSHSession} to the given host. The user and private key associated with the one returned by
   * {@link #getSSHPublicKey()} will be used for authentication.
   *
   * @param host hostname to ssh to
   * @return a new {@link SSHSession}
   * @throws IOException if failed to create a new session to the host
   */
  default SSHSession createSSHSession(String host)  throws IOException {
    return createSSHSession(host, 22, Collections.emptyMap());
  }

  /**
   * Creates a {@link SSHSession} to the given host with extra sets of ssh configurations.
   * The user and private key associated with the one returned by {@link #getSSHPublicKey()} will
   * be used for authentication.
   *
   * @param host hostname to ssh to
   * @param configs set of extra configurations
   * @return a new {@link SSHSession}
   * @throws IOException if failed to create a new session to the host
   */
  default SSHSession createSSHSession(String host, Map<String, String> configs)  throws IOException {
    return createSSHSession(host, 22, configs);
  }

  /**
   * Creates a {@link SSHSession} to the given host, on a specific host, and with extra sets of ssh configurations.
   * The user and private key associated with the one returned by {@link #getSSHPublicKey()} will
   * be used for authentication.
   *
   * @param host hostname to ssh to
   * @param port the port to connect to
   * @param configs set of extra configurations
   * @return a new {@link SSHSession}
   * @throws IOException if failed to create a new session to the host
   */
  SSHSession createSSHSession(String host, int port, Map<String, String> configs) throws IOException;
}
