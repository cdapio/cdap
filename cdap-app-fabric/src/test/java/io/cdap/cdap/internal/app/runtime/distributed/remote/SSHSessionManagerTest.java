/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.gson.Gson;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.common.ssh.TestSSHServer;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitorServerInfo;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ssh.PortForwarding;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import org.apache.twill.common.Cancellable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.CompletableFuture;

/**
 * Unit test for {@link SSHSessionManager}.
 */
public class SSHSessionManagerTest {

  @ClassRule
  public static final TestSSHServer SSH_SERVER = new TestSSHServer();

  private static KeyPair keyPair;

  @BeforeClass
  public static void init() throws IOException, JSchException, GeneralSecurityException {
    keyPair = KeyPair.genKeyPair(new JSch(), KeyPair.RSA, 1024);
    SSH_SERVER.addAuthorizedKey(keyPair, "cdap");
  }

  @Test
  public void testSession() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").spark("spark").run(RunIds.generate());
    SSHSessionManager manager = new SSHSessionManager();

    // Get the session.
    CompletableFuture<SSHSession> sessionFuture = new CompletableFuture<>();
    Cancellable cancellable = manager.addSSHConfig(programRunId, getSSHConfig(), sessionFuture::complete);
    SSHSession session = manager.getSession(programRunId);

    Assert.assertSame(session, sessionFuture.get());

    // The returned session should't allow close
    try {
      session.close();
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // expected
    }

    cancellable.cancel();

    // The session should have been closed
    Assert.assertFalse(session.isAlive());

    // Getting session again will result in exception
    try {
      manager.getSession(programRunId);
      Assert.fail("Excepted IllegalStateException");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testPortForwarding() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").spark("spark").run(RunIds.generate());
    SSHSessionManager manager = new SSHSessionManager();

    ServerSocket serverSocket = new ServerSocket();
    serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    try {
      InetSocketAddress serverAddr = (InetSocketAddress) serverSocket.getLocalSocketAddress();

      // Add a session
      CompletableFuture<SSHSession> sessionFuture = new CompletableFuture<>();
      Cancellable cancellable = manager.addSSHConfig(programRunId, getSSHConfig(), sessionFuture::complete);

      // Port forwarding is not allowed yet
      try {
        manager.createPortForwarding(serverAddr, new PortForwarding.DataConsumer() {
          @Override
          public void received(ByteBuffer buffer) {
            // No-op
          }
        });
        Assert.fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
        // Expected
      }

      // Serialize and deserialize the RuntimeMonitorServerInfo to simulate what's actually happen in remote runtime.
      Gson gson = new Gson();
      RuntimeMonitorServerInfo serverInfo = gson.fromJson(gson.toJson(new RuntimeMonitorServerInfo(serverAddr)),
                                                          RuntimeMonitorServerInfo.class);
      // Add the server address
      manager.addRuntimeServer(programRunId, serverAddr.getAddress(), serverInfo);

      // Now can create the port forwarding
      PortForwarding portForwarding = manager.createPortForwarding(serverAddr, new PortForwarding.DataConsumer() {
        @Override
        public void received(ByteBuffer buffer) {
          // no-op
        }
      });

      // Close out the session
      cancellable.cancel();

      // The port forwarding should have been closed as well
      Assert.assertFalse(portForwarding.isOpen());

    } finally {
      manager.close();
      serverSocket.close();
    }
  }

  private SSHConfig getSSHConfig() {
    return SSHConfig.builder(SSH_SERVER.getHost())
      .setUser("cdap")
      .setPort(SSH_SERVER.getPort())
      .setPrivateKeySupplier(() -> {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        keyPair.writePrivateKey(bos, null);
        return bos.toByteArray();
      })
      .build();
  }
}
