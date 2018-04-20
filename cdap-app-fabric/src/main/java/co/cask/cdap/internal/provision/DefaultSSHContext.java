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

import co.cask.cdap.common.ssh.DefaultSSHSession;
import co.cask.cdap.common.ssh.SSHConfig;
import co.cask.cdap.runtime.spi.ssh.SSHContext;
import co.cask.cdap.runtime.spi.ssh.SSHPublicKey;
import co.cask.cdap.runtime.spi.ssh.SSHSession;

import java.io.IOException;
import java.util.Map;

/**
 * Default implementation of {@link SSHContext}.
 */
public class DefaultSSHContext implements SSHContext {

  private final SSHKeyInfo sshKeyInfo;

  public DefaultSSHContext(SSHKeyInfo sshKeyInfo) {
    this.sshKeyInfo = sshKeyInfo;
  }

  @Override
  public SSHPublicKey getSSHPublicKey() {
    return new SSHPublicKey(sshKeyInfo.getUsername(), sshKeyInfo.getPublicKey());
  }

  @Override
  public SSHSession createSSHSession(String host, int port, Map<String, String> configs) throws IOException {
    SSHConfig config = SSHConfig.builder(host)
      .setPort(port)
      .addConfigs(configs)
      .setUser(sshKeyInfo.getUsername())
      .setPrivateKey(sshKeyInfo.getPrivateKey())
      .build();

    return new DefaultSSHSession(config);
  }
}
