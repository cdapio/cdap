/*
 * Copyright © 2025 Cask Data, Inc.
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

import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ssh.PortForwarding;
import io.cdap.cdap.runtime.spi.ssh.RemotePortForwarding;
import io.cdap.cdap.runtime.spi.ssh.SSHProcess;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.apache.twill.common.Threads;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SSHRemoteExecutionService}.
 */
// CHECKSTYLE.SUPPRESS: AbbreviationAsWordInName
public class SSHRemoteExecutionServiceTest {

  private ScheduledExecutorService scheduler;

  /**
   * Initializes a single-threaded daemon scheduler for each test.
   */
  @Before
  public void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("ssh-exec-test-%d"));
  }

  /**
   * Shuts down the test scheduler.
   */
  @After
  public void tearDown() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void testDoShutdownHandlesNullHealthyAndIoFailingSessionsQuietly() throws Exception {
    SSHRemoteExecutionService service = createServiceWithClosedLocalPort();

    // Case 1: sshSession == null must not throw NullPointerException
    service.doShutdown();

    // Case 2: healthy sshSession is closed cleanly
    FakeSshSession healthySession = new FakeSshSession(false, null);
    setSshSession(service, healthySession);
    service.doShutdown();
    Assert.assertEquals(1, healthySession.closeCount);

    // Case 3: sshSession.close() throwing IOException must be swallowed quietly
    FakeSshSession failingCloseSession =
        new FakeSshSession(false, new IOException("broken ssh pipe on shutdown"));
    setSshSession(service, failingCloseSession);
    service.doShutdown();
    Assert.assertEquals(1, failingCloseSession.closeCount);
  }

  @Test
  public void testDoRunTaskHandlesAliveNullAndIoFailingStaleSessionsQuietly() throws Exception {
    SSHRemoteExecutionService service = createServiceWithClosedLocalPort();

    // Case 1: alive sshSession returns immediately without closing or reconnecting
    FakeSshSession aliveSession = new FakeSshSession(true, null);
    setSshSession(service, aliveSession);
    service.doRunTask();
    Assert.assertEquals(0, aliveSession.closeCount);

    // Case 2: sshSession == null must not throw NullPointerException in closeQuietly,
    // and must proceed to createServiceProxyTunnel() (failing fast on closed local port)
    setSshSession(service, null);
    try {
      service.doRunTask();
      Assert.fail("Expected IOException from createServiceProxyTunnel on closed port");
    } catch (IOException expected) {
      Assert.assertNotNull(expected.getMessage());
    }

    // Case 3: dead sshSession whose close() throws IOException must swallow that close IOException
    // and proceed to createServiceProxyTunnel() instead of aborting early with closeFailure
    IOException closeFailure = new IOException("stale session close io failure");
    FakeSshSession deadFailingCloseSession = new FakeSshSession(false, closeFailure);
    setSshSession(service, deadFailingCloseSession);

    try {
      service.doRunTask();
      Assert.fail("Expected IOException from createServiceProxyTunnel on closed port");
    } catch (IOException thrown) {
      Assert.assertNotSame(closeFailure, thrown);
    }
    Assert.assertEquals(1, deadFailingCloseSession.closeCount);
  }

  private SSHRemoteExecutionService createServiceWithClosedLocalPort() throws Exception {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    SSHConfig sshConfig = SSHConfig.builder("127.0.0.1")
        .setPort(closedPort)
        .setUser("cdap")
        .setPrivateKeySupplier(() -> "dummy-key".getBytes(StandardCharsets.UTF_8))
        .build();
    ProgramRunId runId = NamespaceId.DEFAULT.app("app").spark("sparkJob").run(RunIds.generate());
    return new SSHRemoteExecutionService(
        CConfiguration.create(),
        runId,
        sshConfig,
        1080,
        Mockito.mock(RemoteProcessController.class),
        Mockito.mock(ProgramStateWriter.class),
        scheduler);
  }

  private static void setSshSession(SSHRemoteExecutionService service, SSHSession session)
      throws Exception {
    Field field = SSHRemoteExecutionService.class.getDeclaredField("sshSession");
    field.setAccessible(true);
    field.set(service, session);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
    throw (E) e;
  }

  private static final class FakeSshSession implements SSHSession {

    private final boolean alive;
    private final IOException closeFailure;
    private int closeCount;

    private FakeSshSession(boolean alive, @Nullable IOException closeFailure) {
      this.alive = alive;
      this.closeFailure = closeFailure;
    }

    @Override
    public boolean isAlive() {
      return alive;
    }

    @Override
    public InetSocketAddress getAddress() {
      return InetSocketAddress.createUnresolved("127.0.0.1", 22);
    }

    @Override
    public String getUsername() {
      return "cdap";
    }

    @Override
    public SSHProcess execute(List<String> commands) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String executeAndWait(List<String> commands) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void copy(Path sourceFile, String targetPath) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void copy(
        InputStream input,
        String targetPath,
        String targetName,
        long size,
        int permission,
        @Nullable Long lastAccessTime,
        @Nullable Long lastModifiedTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PortForwarding createLocalPortForward(
        String targetHost,
        int targetPort,
        int originatePort,
        PortForwarding.DataConsumer dataConsumer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RemotePortForwarding createRemotePortForward(int remotePort, int localPort) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      closeCount++;
      if (closeFailure != null) {
        sneakyThrow(closeFailure);
      }
    }
  }
}
