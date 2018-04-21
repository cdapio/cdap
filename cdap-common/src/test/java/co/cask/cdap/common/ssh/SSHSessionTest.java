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


import co.cask.cdap.runtime.spi.ssh.SSHSession;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.config.keys.AuthorizedKeysAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.scp.ScpCommandFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Unit tests for {@link SSHSession}.
 */
public class SSHSessionTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static SshServer sshd;
  private static KeyPair keyPair;

  @BeforeClass
  public static void init() throws IOException, JSchException {
    keyPair = KeyPair.genKeyPair(new JSch(), KeyPair.RSA, 1024);

    File authorizedKeysFile = TEMP_FOLDER.newFile();
    keyPair.writePublicKey(authorizedKeysFile.getAbsolutePath(), "cdap@cask.co");

    sshd = SshServer.setUpDefaultServer();
    sshd.setHost(InetAddress.getLoopbackAddress().getCanonicalHostName());
    sshd.setPort(0);
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
    sshd.setPublickeyAuthenticator(new AuthorizedKeysAuthenticator(authorizedKeysFile));
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
      public void destroy() throws Exception {

      }
    }).build());

    sshd.start();
  }

  @AfterClass
  public static void finish() throws IOException {
    sshd.stop();
  }

  @Test
  public void testScp() throws Exception {
    SSHConfig config = getSSHConfig();

    // Generate some content
    File file = TEMP_FOLDER.newFile();
    try (BufferedWriter writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      for (int i = 0; i < 10; i++) {
        writer.write("Message " + i);
        writer.newLine();
      }
    }

    // SCP the file to the given directory
    File targetFolder = TEMP_FOLDER.newFolder();
    try (SSHSession session = new DefaultSSHSession(config)) {
      session.copy(file.toPath(), targetFolder.getAbsolutePath());
    }

    // Verify
    File uploadedFile = new File(targetFolder, file.getName());
    Assert.assertTrue(uploadedFile.exists());
    Assert.assertArrayEquals(Files.readAllBytes(file.toPath()), Files.readAllBytes(uploadedFile.toPath()));
  }

  @Test
  public void testSsh() throws Exception {
    SSHConfig config = getSSHConfig();

    try (SSHSession session = new DefaultSSHSession(config)) {
      for (int i = 0; i < 10; i++) {
        String msg = "Sending some message " + i;
        String result = session.executeAndWait(msg);
        Assert.assertEquals(msg, result);
      }
    }

    // Test the error exit
    try (SSHSession session = new DefaultSSHSession(config)) {
      try {
        session.executeAndWait("failure");
        Assert.fail("Expected failure from ssh command");
      } catch (Exception e) {
        // Expected
      }
    }
  }

  private SSHConfig getSSHConfig() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    keyPair.writePrivateKey(bos, null);

    return SSHConfig.builder(sshd.getHost())
      .setUser("cdap")
      .setPort(sshd.getPort())
      .setPrivateKey(bos.toByteArray())
      .build();
  }
}
