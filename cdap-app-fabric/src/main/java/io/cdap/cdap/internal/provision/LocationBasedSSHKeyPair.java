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

import com.google.common.io.ByteStreams;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * A {@link SSHKeyPair} that loads public and private keys from a given {@link Location}.
 */
public class LocationBasedSSHKeyPair extends SSHKeyPair {

  public LocationBasedSSHKeyPair(Location keysDir, String sshUser) throws IOException {
    super(createSSHPublicKey(keysDir, sshUser), createPrivateKeySupplier(keysDir));
  }

  private static SSHPublicKey createSSHPublicKey(Location keysDir, String sshUser) throws IOException  {
    try (InputStream is = keysDir.append(Constants.RuntimeMonitor.PUBLIC_KEY).getInputStream()) {
      return new SSHPublicKey(sshUser, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }
  }

  private static Supplier<byte[]> createPrivateKeySupplier(Location keysDir) {
    return () -> {
      try (InputStream input = keysDir.append(Constants.RuntimeMonitor.PRIVATE_KEY).getInputStream()) {
        return ByteStreams.toByteArray(input);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load private key", e);
      }
    };
  }
}
