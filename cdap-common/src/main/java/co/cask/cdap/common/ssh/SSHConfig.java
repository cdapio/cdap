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

package co.cask.cdap.common.ssh;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Configurations for creating a {@link DefaultSSHSession}.
 */
public final class SSHConfig {

  private final String host;
  private final int port;

  private final String user;
  private final Supplier<byte[]> privateKeySupplier;
  private final Map<String, String> configs;

  private SSHConfig(String host, int port, String user,
                    Supplier<byte[]> privateKeySupplier, Map<String, String> configs) {
    this.host = host;
    this.user = user;
    this.privateKeySupplier = privateKeySupplier;
    this.port = port;
    this.configs = Collections.unmodifiableMap(new HashMap<>(configs));
  }

  /**
   * Returns the host to ssh to.
   */
  public String getHost() {
    return host;
  }

  /**
   * Returns the port used for ssh.
   */
  public int getPort() {
    return port;
  }

  /**
   * Returns the user name to use for ssh.
   */
  public String getUser() {
    return user;
  }

  /**
   * Returns the private key for login.
   */
  byte[] getPrivateKey() {
    return privateKeySupplier.get();
  }

  /**
   * Returns the ssh configuration to use for the SSH session.
   */
  public Map<String, String> getConfigs() {
    return configs;
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @param host the host to ssh to.
   * @return a new instance of {@link Builder}.
   */
  public static Builder builder(String host) {
    return new Builder(host);
  }

  /**
   * A builder for constructing {@link SSHConfig}.
   */
  public static final class Builder {

    private final String host;
    private final Map<String, String> configs = new HashMap<>();
    private int port = 22;
    private String user;
    private Supplier<byte[]> privateKeySupplier;

    private Builder(String host) {
      this.host = host;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setPrivateKeySupplier(Supplier<byte[]> privateKeySupplier) {
      this.privateKeySupplier = privateKeySupplier;
      return this;
    }

    public Builder addConfig(String key, String value) {
      this.configs.put(key, value);
      return this;
    }

    public Builder addConfigs(Map<String, String> configs) {
      this.configs.putAll(configs);
      return this;
    }

    public SSHConfig build() {
      return new SSHConfig(host, port, user, privateKeySupplier, configs);
    }
  }
}
