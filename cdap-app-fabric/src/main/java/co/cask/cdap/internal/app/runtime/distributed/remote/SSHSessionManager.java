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

package co.cask.cdap.internal.app.runtime.distributed.remote;

import co.cask.cdap.common.ssh.DefaultSSHSession;
import co.cask.cdap.common.ssh.SSHConfig;
import co.cask.cdap.internal.app.runtime.monitor.SSHSessionProvider;
import co.cask.cdap.internal.app.runtime.monitor.proxy.MonitorSocksProxy;
import co.cask.cdap.runtime.spi.ssh.PortForwarding;
import co.cask.cdap.runtime.spi.ssh.RemotePortForwarding;
import co.cask.cdap.runtime.spi.ssh.SSHProcess;
import co.cask.cdap.runtime.spi.ssh.SSHSession;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;

/**
 * Manages SSH sessions for the runtime {@link MonitorSocksProxy}.
 */
final class SSHSessionManager implements SSHSessionProvider, AutoCloseable {

  private final ConcurrentMap<InetSocketAddress, SSHInfo> sshInfos;

  SSHSessionManager() {
    this.sshInfos = new ConcurrentHashMap<>();
  }

  /**
   * Adds a {@link SSHConfig} to this manager such that {@link SSHSession} can be acquired from the
   * {@link #getSession(InetSocketAddress)} method that goes to the same host.
   *
   * @param serverAddr the {@link InetSocketAddress} of where the runtime monitor server is running
   * @param sshConfig the {@link SSHConfig} to add
   */
  void addSSHConfig(InetSocketAddress serverAddr, SSHConfig sshConfig) {
    sshInfos.putIfAbsent(serverAddr, new SSHInfo(sshConfig));
  }

  /**
   * Removes a {@link SSHConfig} from this manager such that {@link SSHSession} cannot be acquired from the
   * {@link #getSession(InetSocketAddress)} method that goes to the same host.
   * This method will also InetSocketAddress the active {@link SSHSession} managed by this manager that is
   * associated with the given {@link SSHConfig}.
   *
   * @param serverAddr the {@link InetSocketAddress} of where the runtime monitor server is running
   */
  void removeSSHConfig(InetSocketAddress serverAddr) {
    SSHInfo info = sshInfos.remove(serverAddr);
    CloseDisabledSSHSession session = info.getSession();
    if (session != null) {
      session.getDelegate().close();
    }
  }

  /**
   * Close this manager by close all {@link SSHSession}s that are managed by this class.
   */
  @Override
  public void close() {
    for (SSHInfo info : sshInfos.values()) {
      CloseDisabledSSHSession session = info.getSession();
      if (session != null) {
        session.getDelegate().close();
      }
    }
    sshInfos.clear();
  }

  @Override
  public SSHSession getSession(InetSocketAddress serverAddr) {
    SSHSession session = getAliveSession(serverAddr);
    if (session != null) {
      return session;
    }

    synchronized (this) {
      // Check again to make sure we don't create multiple SSHSession
      session = getAliveSession(serverAddr);
      if (session != null) {
        return session;
      }

      try {
        SSHInfo sshInfo = sshInfos.get(serverAddr);
        if (sshInfo == null) {
          throw new IllegalStateException("No SSHSession available for " + serverAddr);
        }
        SSHConfig config = sshInfo.getConfig();

        session = new DefaultSSHSession(config) {
          @Override
          public void close() {
            // On closing of the ssh session, replace the SSHInfo with a null session
            // We do replace such that if the SSHInfo was removed, we won't add it back.
            sshInfos.replace(serverAddr, new SSHInfo(config));
          }
        };

        // Replace the SSHInfo. If the replacement was not successful, it means the removeSSHConfig was called
        // in between, hence we should close the new session and throw exception.
        // It is also possible that the info was removed and then added back.
        // In that case, we still treat that it is no longer valid to create the SSH session using the old config.
        CloseDisabledSSHSession resultSession = new CloseDisabledSSHSession(session);
        if (!sshInfos.replace(serverAddr, sshInfo, new SSHInfo(config, resultSession))) {
          session.close();
          throw new IllegalStateException("No SSHSession available for " + serverAddr);
        }
        return resultSession;
      } catch (IOException e) {
        throw new IllegalStateException("Failed to create SSHSession for " + serverAddr);
      }
    }
  }

  /**
   * Returns an existing {@link SSHSession} for the given host.
   *
   * @param serverAddr the {@link InetSocketAddress} of where the runtime monitor server is running
   * @return a {@link SSHSession} or {@code null} if no existing {@link SSHSession} are available.
   */
  @Nullable
  private SSHSession getAliveSession(InetSocketAddress serverAddr) {
    SSHInfo sshInfo = sshInfos.get(serverAddr);
    if (sshInfo == null) {
      throw new IllegalStateException("No SSHSession available for " + serverAddr);
    }

    SSHSession session = sshInfo.getSession();
    if (session == null) {
      return null;
    }

    if (session.isAlive()) {
      return session;
    }

    // If the session is not alive, remove it from the map by replacing the value with a SSHInfo that
    // doesn't have SSHSession.
    sshInfos.replace(serverAddr, sshInfo, new SSHInfo(sshInfo.getConfig()));
    return null;
  }

  /**
   * A class that contains a {@link SSHConfig} and a {@link SSHSession}.
   */
  private static final class SSHInfo {

    private final SSHConfig config;
    private final CloseDisabledSSHSession session;

    SSHInfo(SSHConfig config) {
      this(config, null);
    }

    SSHInfo(SSHConfig config, @Nullable CloseDisabledSSHSession session) {
      this.config = config;
      this.session = session;
    }

    SSHConfig getConfig() {
      return config;
    }

    @Nullable
    CloseDisabledSSHSession getSession() {
      return session;
    }
  }

  /**
   * A {@link SSHSession} wrapper that forward calls to another {@link SSHSession},
   * with the {@link #close()} method disabled.
   */
  private static final class CloseDisabledSSHSession implements SSHSession {

    private final SSHSession delegate;

    CloseDisabledSSHSession(SSHSession delegate) {
      this.delegate = delegate;
    }

    SSHSession getDelegate() {
      return delegate;
    }

    @Override
    public boolean isAlive() {
      return getDelegate().isAlive();
    }

    @Override
    public InetSocketAddress getAddress() {
      return getDelegate().getAddress();
    }

    @Override
    public String getUsername() {
      return getDelegate().getUsername();
    }

    @Override
    public SSHProcess execute(List<String> commands) throws IOException {
      return getDelegate().execute(commands);
    }

    @Override
    public String executeAndWait(List<String> commands) throws IOException {
      return getDelegate().executeAndWait(commands);
    }

    @Override
    public void copy(Path sourceFile, String targetPath) throws IOException {
      getDelegate().copy(sourceFile, targetPath);
    }

    @Override
    public void copy(InputStream input, String targetPath, String targetName, long size, int permission,
                     @Nullable Long lastAccessTime, @Nullable Long lastModifiedTime) throws IOException {
      getDelegate().copy(input, targetPath, targetName, size, permission, lastAccessTime, lastModifiedTime);
    }

    @Override
    public PortForwarding createLocalPortForward(String targetHost, int targetPort, int originatePort,
                                                 PortForwarding.DataConsumer dataConsumer) throws IOException {
      return getDelegate().createLocalPortForward(targetHost, targetPort, originatePort, dataConsumer);
    }

    @Override
    public RemotePortForwarding createRemotePortForward(int remotePort, int localPort) throws IOException {
      return getDelegate().createRemotePortForward(remotePort, localPort);
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException("Should not close managed SSHSession");
    }
  }
}
