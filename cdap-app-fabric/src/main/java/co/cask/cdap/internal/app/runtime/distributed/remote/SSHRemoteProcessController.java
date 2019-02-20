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
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.provision.ProvisioningService;
import co.cask.cdap.master.spi.program.ProgramOptions;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.ssh.SSHProcess;
import co.cask.cdap.runtime.spi.ssh.SSHSession;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Implementation of {@link RemoteProcessController} that SSH into the remote machine and look for the running process.
 */
final class SSHRemoteProcessController implements RemoteProcessController {

  private static final Logger LOG = LoggerFactory.getLogger(SSHRemoteProcessController.class);
  private static final Gson GSON = new Gson();

  private final ProgramRunId programRunId;
  private final ProgramOptions programOpts;
  private final SSHConfig sshConfig;
  private final ProvisioningService provisioningService;

  SSHRemoteProcessController(ProgramRunId programRunId, ProgramOptions programOpts,
                             SSHConfig sshConfig, ProvisioningService provisioningService) {
    this.programRunId = programRunId;
    this.programOpts = programOpts;
    this.sshConfig = sshConfig;
    this.provisioningService = provisioningService;
  }

  @Override
  public boolean isRunning() throws Exception {
    // Try to SSH into the host and see if the CDAP runtime process is running or not
    try (SSHSession session = new DefaultSSHSession(sshConfig)) {
      SSHProcess process = session.execute("pgrep -f -- -Dcdap.runid=" + programRunId.getRun());

      // Reading will be blocked until the process finished.
      // The output is not needed, just read it to avoid filling up the network buffer.
      ByteStreams.toByteArray(process.getInputStream());
      ByteStreams.toByteArray(process.getErrorStream());

      int exitCode = process.waitFor();
      if (exitCode != 0) {
        LOG.info("Received exit code {} when checking for remote process for program run {}.", exitCode, programRunId);
      }
      return exitCode == 0;
    } catch (IOException e) {
      // If there is error performing SSH, check if the cluster still exist and running
      LOG.debug("Failed to use SSH to determine if the remote process is running for {}. Check cluster status instead.",
                programRunId, e);

      Cluster cluster = GSON.fromJson(programOpts.getArguments().getOption(ProgramOptionConstants.CLUSTER),
                                      Cluster.class);
      String userId = programOpts.getArguments().getOption(ProgramOptionConstants.USER_ID);
      ClusterStatus clusterStatus = provisioningService.getClusterStatus(programRunId, programOpts, cluster, userId);

      // The cluster status has to be RUNNING in order for the remote process still has a chance that is running
      return clusterStatus == ClusterStatus.RUNNING;
    }
  }

  @Override
  public void kill() throws Exception {
    // SSH and kill the process
    try (SSHSession session = new DefaultSSHSession(sshConfig)) {
      SSHProcess process = session.execute("pkill -9 -f -- -Dcdap.runid=" + programRunId.getRun());

      // Reading will be blocked until the process finished
      ByteStreams.toByteArray(process.getInputStream());
      String err = CharStreams.toString(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8));

      int exitCode = process.waitFor();
      // If the exit code is 1, it means there is no such process, which is fine from the killing perspective
      if (exitCode == 0 || exitCode == 1) {
        return;
      }

      throw new IllegalStateException("Failed to kill remote process for program run " + programRunId
                                        + " due to error " + err);
    }
  }
}
