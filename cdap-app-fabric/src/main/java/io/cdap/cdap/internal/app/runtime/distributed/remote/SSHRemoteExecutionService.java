/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.ssh.DefaultSSHSession;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.internal.app.runtime.monitor.ServiceSocksProxyInfo;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The service to maintain a SSH tunnel with remote port forwarding for the remote runtime to access CDAP services
 * via the tunnel.
 */
final class SSHRemoteExecutionService extends RemoteExecutionService {

  private static final Logger LOG = LoggerFactory.getLogger(SSHRemoteExecutionService.class);
  private static final Gson GSON = new Gson();

  private final SSHConfig sshConfig;
  private final int serviceSocksProxyPort;
  private SSHSession sshSession;

  SSHRemoteExecutionService(CConfiguration cConf, ProgramRunId programRunId,
                            SSHConfig sshConfig, int serviceSocksProxyPort,
                            RemoteProcessController remoteProcessController,
                            ProgramStateWriter programStateWriter,
                            ScheduledExecutorService scheduledExecutor) {
    super(cConf, programRunId, scheduledExecutor, remoteProcessController, programStateWriter);
    this.sshConfig = sshConfig;
    this.serviceSocksProxyPort = serviceSocksProxyPort;
  }

  @Override
  protected void doStartUp() throws IOException {
    LOG.debug("Starting ssh service for run {}", getProgramRunId());
    sshSession = createServiceProxyTunnel();
  }

  @Override
  protected void doRunTask() throws Exception {
    if (!sshSession.isAlive()) {
      sshSession.close();
      sshSession = createServiceProxyTunnel();
    }
  }

  @Override
  protected void doShutdown() {
    super.doShutdown();
    if (sshSession != null) {
      sshSession.close();
    }
    LOG.debug("Stopped ssh service for run {}", getProgramRunId());
  }

  /**
   * Creates a remote port forwarding SSH tunnel for remote runtime to access CDAP services.
   */
  private SSHSession createServiceProxyTunnel() throws IOException {
    SSHSession session = new DefaultSSHSession(sshConfig);
    ProgramRunId programRunId = getProgramRunId();

    // Creates a new remote port forwarding from the session.
    // We don't need to care about closing the forwarding as it will last until the session get closed,
    // in which the remote port forwarding will be closed automatically
    int remotePort = session.createRemotePortForward(0, serviceSocksProxyPort).getRemotePort();

    LOG.debug("Service SOCKS proxy started on port {} for program run {}", remotePort, programRunId);
    ServiceSocksProxyInfo info = new ServiceSocksProxyInfo(remotePort);

    // Upload the service socks proxy information to the remote runtime
    String targetPath = session.executeAndWait("echo `pwd`/" + programRunId.getRun()).trim();
    session.executeAndWait("mkdir -p " + targetPath);
    byte[] content = GSON.toJson(info).getBytes(StandardCharsets.UTF_8);
    String targetFileName = Constants.RuntimeMonitor.SERVICE_PROXY_FILE + "-" + programRunId.getRun() + ".json";
    String tmpFileName = targetFileName + "-" + System.currentTimeMillis() + ".tmp";
    //noinspection OctalInteger
    session.copy(new ByteArrayInputStream(content), "/tmp", tmpFileName, content.length, 0600, null, null);
    session.executeAndWait(String.format("mv /tmp/%s /tmp/%s", tmpFileName, targetFileName));
    LOG.debug("Service proxy file uploaded to remote runtime for program run {}", programRunId);

    return session;
  }
}
