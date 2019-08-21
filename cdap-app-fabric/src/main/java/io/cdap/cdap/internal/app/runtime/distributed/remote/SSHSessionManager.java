/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import com.google.common.io.Closeables;
import io.cdap.cdap.common.ssh.DefaultSSHSession;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitorServerInfo;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.MonitorSocksProxy;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.PortForwardingProvider;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ssh.PortForwarding;
import io.cdap.cdap.runtime.spi.ssh.RemotePortForwarding;
import io.cdap.cdap.runtime.spi.ssh.SSHProcess;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import org.apache.twill.common.Cancellable;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Manages SSH sessions for the runtime {@link MonitorSocksProxy}.
 */
final class SSHSessionManager implements PortForwardingProvider, AutoCloseable {

  private final ConcurrentMap<ProgramRunId, SSHInfo> sshInfos;
  private final ConcurrentMap<InetAddress, ProgramRunId> allowedPortForwardings;
  private final ConcurrentMap<ProgramRunId, RuntimeMonitorServerInfo> monitorServerInfos;

  SSHSessionManager() {
    this.sshInfos = new ConcurrentHashMap<>();
    this.allowedPortForwardings = new ConcurrentHashMap<>();
    this.monitorServerInfos = new ConcurrentHashMap<>();
  }

  /**
   * Adds a {@link SSHConfig} for the given program run
   *
   * @param programRunId the {@link ProgramRunId} for the run that running in the given cluster
   * @param sshConfig the {@link SSHConfig} for where the program is running
   * @param sessionConsumer a {@link Consumer} that gets called every time when a new {@link SSHSession} is created
   *                        for the given program run id
   * @return a {@link Cancellable} to remove the program run SSH config
   */
  Cancellable addSSHConfig(ProgramRunId programRunId, SSHConfig sshConfig, Consumer<SSHSession> sessionConsumer) {
    sshInfos.put(programRunId, new SSHInfo(sshConfig, sessionConsumer));
    return () ->
      Optional.ofNullable(sshInfos.remove(programRunId))
        .map(SSHInfo::getSession)
        .map(CloseDisabledSSHSession::getDelegate)
        .ifPresent(Closeables::closeQuietly);
  }

  /**
   * Associates the remote runtime server address with the given program run.
   *
   * @param programRunId the {@link ProgramRunId} to have the runtime server information added
   * @param allowedAddr the {@link InetAddress} where remote runtime server where program run is running on
   * @param serverInfo the {@link RuntimeMonitorServerInfo} about the remote runtime server binding information
   */
  void addRuntimeServer(ProgramRunId programRunId, InetAddress allowedAddr, RuntimeMonitorServerInfo serverInfo) {
    monitorServerInfos.put(programRunId, serverInfo);
    allowedPortForwardings.put(allowedAddr, programRunId);
  }

  /**
   * Removes the remote runtime server address with the given program run.
   *
   * @param programRunId the {@link ProgramRunId} to have the runtime server information removed
   * @param allowedAddr the {@link InetAddress} of where the runtime monitor server is running
   */
  void removeRuntimeServer(ProgramRunId programRunId, InetAddress allowedAddr) {
    allowedPortForwardings.remove(allowedAddr, programRunId);
    monitorServerInfos.remove(programRunId);
  }

  /**
   * Close this manager by close all {@link SSHSession}s that are managed by this class.
   */
  @Override
  public void close() {
    sshInfos.values().stream()
      .map(SSHInfo::getSession)
      .filter(Objects::nonNull)
      .map(CloseDisabledSSHSession::getDelegate)
      .forEach(Closeables::closeQuietly);
    sshInfos.clear();
  }

  @Override
  public PortForwarding createPortForwarding(InetSocketAddress serverAddr,
                                             PortForwarding.DataConsumer dataConsumer) throws IOException {
    ProgramRunId programRunId = allowedPortForwardings.get(serverAddr.getAddress());
    if (programRunId == null) {
      throw new IllegalStateException("No SSHSession available for server " + serverAddr);
    }
    RuntimeMonitorServerInfo serverInfo = monitorServerInfos.get(programRunId);
    if (serverInfo == null) {
      throw new IllegalStateException("No runtime monitor information available for program run " + programRunId);
    }
    if (serverAddr.getPort() != serverInfo.getPort()) {
      throw new IllegalStateException("Runtime monitor server is listening on port " + serverInfo.getPort()
                                        + ", which is different than the request port " + serverAddr.getPort());
    }
    return getSession(programRunId).createLocalPortForward(serverInfo.getHostAddress().getHostAddress(),
                                                           serverInfo.getPort(),
                                                           serverAddr.getPort(), dataConsumer);
  }

  /**
   * Returns an alive {@link SSHSession} for the given program run.
   */
  SSHSession getSession(ProgramRunId programRunId) {
    SSHSession session = getAliveSession(programRunId);
    if (session != null) {
      return session;
    }

    synchronized (this) {
      // Check again to make sure we don't create multiple SSHSession
      session = getAliveSession(programRunId);
      if (session != null) {
        return session;
      }

      try {
        SSHInfo sshInfo = sshInfos.get(programRunId);
        if (sshInfo == null) {
          throw new IllegalStateException("No SSHSession available for run " + programRunId);
        }
        SSHConfig config = sshInfo.getConfig();
        Consumer<SSHSession> sessionConsumer = sshInfo.getSessionConsumer();

        session = new DefaultSSHSession(config) {
          @Override
          public void close() {
            // On closing of the ssh session, replace the SSHInfo with a null session
            // We do replace such that if the SSHInfo was removed, we won't add it back.
            sshInfos.replace(programRunId, new SSHInfo(config, sessionConsumer));
            super.close();
          }
        };

        // Call the session consumer. If failed, close the session and rethrow
        CloseDisabledSSHSession resultSession = new CloseDisabledSSHSession(session);
        try {
          sessionConsumer.accept(resultSession);
        } catch (Exception e) {
          Closeables.closeQuietly(session);
          throw e;
        }

        // Replace the SSHInfo. If the replacement was not successful, it means the removeSSHConfig was called
        // in between, hence we should close the new session and throw exception.
        // It is also possible that the info was removed and then added back.
        // In that case, we still treat that it is no longer valid to create the SSH session using the old config.
        if (!sshInfos.replace(programRunId, sshInfo, new SSHInfo(config, sessionConsumer, resultSession))) {
          Closeables.closeQuietly(session);
          throw new IllegalStateException("No SSHSession available for run " + programRunId);
        }
        return resultSession;
      } catch (IOException e) {
        throw new IllegalStateException("Failed to create SSHSession for run " + programRunId, e);
      }
    }
  }

  /**
   * Returns an existing {@link SSHSession} for the given run.
   *
   * @param programRunId the {@link ProgramRunId} to get the SSH session
   * @return a {@link SSHSession} or {@code null} if no existing {@link SSHSession} are available
   * @throws IllegalStateException if there is no SSH information associated with the given run
   */
  @Nullable
  private SSHSession getAliveSession(ProgramRunId programRunId) {
    SSHInfo sshInfo = sshInfos.get(programRunId);
    if (sshInfo == null) {
      throw new IllegalStateException("No SSHSession available for run " + programRunId);
    }

    CloseDisabledSSHSession session = sshInfo.getSession();
    if (session == null) {
      return null;
    }

    if (session.isAlive()) {
      return session;
    }

    // Close the not alive session
    session.getDelegate().close();

    // If the session is not alive, replacing the value with a SSHInfo that doesn't have SSHSession.
    sshInfos.replace(programRunId, sshInfo, new SSHInfo(sshInfo.getConfig(), sshInfo.getSessionConsumer()));
    return null;
  }

  /**
   * A class that contains a {@link SSHConfig} and a {@link SSHSession}.
   */
  private static final class SSHInfo {

    private final SSHConfig config;
    private final Consumer<SSHSession> sessionConsumer;
    private final CloseDisabledSSHSession session;

    SSHInfo(SSHConfig config, Consumer<SSHSession> sessionConsumer) {
      this(config, sessionConsumer, null);
    }

    SSHInfo(SSHConfig config, Consumer<SSHSession> sessionConsumer, @Nullable CloseDisabledSSHSession session) {
      this.config = config;
      this.sessionConsumer = sessionConsumer;
      this.session = session;
    }

    SSHConfig getConfig() {
      return config;
    }

    Consumer<SSHSession> getSessionConsumer() {
      return sessionConsumer;
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
