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

package io.cdap.cdap.internal.provision;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.ssh.DefaultSSHSession;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import org.apache.twill.filesystem.Location;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link SSHContext}.
 */
public class DefaultSSHContext implements SSHContext {

  private final InetSocketAddress proxyAddress;

  @Nullable
  private final Location keysDir;
  @Nullable
  private SSHKeyPair sshKeyPair;

  DefaultSSHContext(@Nullable InetSocketAddress proxyAddress,
                    @Nullable Location keysDir, @Nullable SSHKeyPair keyPair) {
    this.proxyAddress = proxyAddress;
    this.keysDir = keysDir;
    this.sshKeyPair = keyPair;
  }

  @Override
  public SSHKeyPair generate(String user, int bits) throws KeyException {
    JSch jsch = new JSch();
    try {
      KeyPair keyPair = KeyPair.genKeyPair(jsch, KeyPair.RSA, bits);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      keyPair.writePublicKey(bos, user);
      SSHPublicKey publicKey = new SSHPublicKey(user, new String(bos.toByteArray(), StandardCharsets.UTF_8));

      bos.reset();
      keyPair.writePrivateKey(bos);
      byte[] privateKey = bos.toByteArray();

      return new SSHKeyPair(publicKey, () -> privateKey);
    } catch (JSchException e) {
      throw new KeyException("Failed to generate ssh key pair", e);
    }
  }

  @Override
  public void setSSHKeyPair(SSHKeyPair keyPair) {
    if (keysDir == null) {
      throw new IllegalStateException("Setting of key pair is not allowed. " +
                                        "It can only be called during the Provisioner.createCluster cycle");
    }
    this.sshKeyPair = keyPair;

    // Save the ssh key pair
    try {
      Location publicKeyFile = keysDir.append(Constants.RuntimeMonitor.PUBLIC_KEY);
      try (OutputStream os = publicKeyFile.getOutputStream()) {
        os.write(keyPair.getPublicKey().getKey().getBytes(StandardCharsets.UTF_8));
      }

      Location privateKeyFile = keysDir.append(Constants.RuntimeMonitor.PRIVATE_KEY);
      try (OutputStream os = privateKeyFile.getOutputStream("600")) {
        os.write(keyPair.getPrivateKeySupplier().get());
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to save the ssh key pair", e);
    }
  }

  @Override
  public Optional<SSHKeyPair> getSSHKeyPair() {
    return Optional.ofNullable(sshKeyPair);
  }

  @Override
  public SSHSession createSSHSession(String user, Supplier<byte[]> privateKeySupplier,
                                     String host, int port, Map<String, String> configs) throws IOException {
    SSHConfig config = SSHConfig.builder(host)
      .setPort(port)
      .setProxyAddress(proxyAddress)
      .setUser(user)
      .setPrivateKeySupplier(privateKeySupplier)
      .build();

    return new DefaultSSHSession(config);
  }
}
