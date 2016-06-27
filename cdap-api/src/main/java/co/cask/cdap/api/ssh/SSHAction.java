/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.ssh;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 */
public class SSHAction extends AbstractWorkflowAction {

  private class SSHConfig {
    // Server information
    private String host;
    private int port;

    // Authentication information
    private boolean usePasswordSSH;
    private String user;
    private String password;
    private String privateKeyFile;
    private String privateKeyPassphrase;
    private String knownHostsFile;

    private String cmd;

    public SSHConfig(boolean usePasswordSSH, String host, int port, String user, String password, String privateKeyFile,
                     String privateKeyPassphrase, String cmd) {
      this.usePasswordSSH = usePasswordSSH;
      this.host = host;
      this.port = port;
      this.user = user;
      this.password = password;
      this.privateKeyFile = privateKeyFile;
      this.privateKeyPassphrase = privateKeyPassphrase;
      this.cmd = cmd;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SSHAction.class);

  private SSHConfig config;
  private boolean establishedConnection = false;

  public SSHAction(boolean usePasswordSSH, String host, int port, String user, String password, String privateKeyFile,
                   String privateKeyPassphrase, String cmd) {
    this.config = new SSHConfig(usePasswordSSH, host, port, user, password, privateKeyFile, privateKeyPassphrase, cmd);
  }

  @Override
  public void run() {
    try {
      Connection connection = new Connection(config.host);
      connection.connect();

      // Authenticate with password or private key file
      if (config.usePasswordSSH) {
        establishedConnection = connection.authenticateWithPassword(config.user, config.password);
      } else {
        File privateKey = new File(config.privateKeyFile);
        establishedConnection = connection.authenticateWithPublicKey(config.user, privateKey,
                                                                     config.privateKeyPassphrase);
      }

      if (!establishedConnection()) {
        throw new IOException(String.format("Unable to establish SSH connection for %s@%s on port %d",
                                            config.user, config.host, config.port));
      }

      Session session = connection.openSession();
      session.execCommand(config.cmd);

      // Read stdout and stderr
      InputStream stdout = new StreamGobbler(session.getStdout());
      BufferedReader outBuffer = new BufferedReader(new InputStreamReader(stdout));
      StringBuilder outBuilder = new StringBuilder();
      InputStream stderr = new StreamGobbler(session.getStderr());
      BufferedReader errBuffer = new BufferedReader(new InputStreamReader(stderr));
      StringBuilder errBuilder = new StringBuilder();

      String line = outBuffer.readLine();
      while (line != null) {
        outBuilder.append(line + "\n");
        line = outBuffer.readLine();
      }

      line = errBuffer.readLine();
      while (line != null) {
        errBuilder.append(line + "\n");
        line = errBuffer.readLine();
      }

      LOG.info("Finished running command {} for {}@{} on port {}.", config.cmd, config.user, config.host,
               config.port);
      LOG.info("Output:");
      LOG.info(outBuilder.toString());
      LOG.info("Errors:");
      LOG.info(errBuilder.toString());

      session.close();
    } catch (IOException e) {
      LOG.error("Unable to establish connection.", e);
    }
  }

  public boolean establishedConnection() {
    return establishedConnection;
  }
}
