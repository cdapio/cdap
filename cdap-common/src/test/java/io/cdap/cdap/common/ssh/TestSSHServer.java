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

package io.cdap.cdap.common.ssh;

import com.jcraft.jsch.KeyPair;
import org.apache.sshd.common.config.keys.AuthorizedKeyEntry;
import org.apache.sshd.common.config.keys.PublicKeyEntryResolver;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.pubkey.KeySetPublickeyAuthenticator;
import org.apache.sshd.server.forward.AcceptAllForwardingFilter;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.scp.ScpCommandFactory;
import org.junit.rules.ExternalResource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A SSH server for testing purpose.
 */
public class TestSSHServer extends ExternalResource {

  private final Set<PublicKey> authorizedKeys = Collections.synchronizedSet(new HashSet<>());
  private SshServer sshd;

  @Override
  protected void before() throws Throwable {
    sshd = SshServer.setUpDefaultServer();
    sshd.setHost(InetAddress.getLoopbackAddress().getCanonicalHostName());
    sshd.setPort(0);
    sshd.setForwardingFilter(AcceptAllForwardingFilter.INSTANCE);
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
    sshd.setPublickeyAuthenticator(new KeySetPublickeyAuthenticator(authorizedKeys));

    // Support SCP and a CommandFactory that always response back the request command
    sshd.setCommandFactory(new ScpCommandFactory.Builder().withDelegate(command -> new Command() {

      private OutputStream out;
      private OutputStream err;
      private ExitCallback callback;

      @Override
      public void setInputStream(InputStream in) {
      }

      @Override
      public void setOutputStream(OutputStream out) {
        this.out = out;
      }

      @Override
      public void setErrorStream(OutputStream err) {
        this.err = err;
      }

      @Override
      public void setExitCallback(ExitCallback callback) {
        this.callback = callback;
      }

      @Override
      public void start(Environment env) throws IOException {
        // Just echo the command back and terminate

        // If the command contains "fail", then echo back to the error stream with non-zero exit code
        boolean failure = command.contains("fail");
        OutputStream output = failure ? err : out;
        output.write(command.getBytes(StandardCharsets.UTF_8));
        output.flush();

        callback.onExit(failure ? 1 : 0);
      }

      @Override
      public void destroy() {

      }
    }).build());

    sshd.start();

  }

  @Override
  protected void after() {
    try {
      sshd.stop();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Adds an authorized public key from the given {@link KeyPair}.
   */
  public void addAuthorizedKey(KeyPair keyPair, String user) throws IOException, GeneralSecurityException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    keyPair.writePublicKey(bos, user);
    AuthorizedKeyEntry entry = AuthorizedKeyEntry.parseAuthorizedKeyEntry(new String(bos.toByteArray(),
                                                                                     StandardCharsets.US_ASCII));
    addAuthorizedKey(entry.resolvePublicKey(PublicKeyEntryResolver.IGNORING));
  }

  /**
   * Adds the given {@link PublicKey} to the authorized keys.
   */
  public void addAuthorizedKey(PublicKey publicKey) {
    authorizedKeys.add(publicKey);
  }

  /**
   * Returns the host that the SSH server is bind to.
   */
  public String getHost() {
    return sshd.getHost();
  }

  /**
   * Returns the port that the SSH server is bind to.
   */
  public int getPort() {
    return sshd.getPort();
  }
}
