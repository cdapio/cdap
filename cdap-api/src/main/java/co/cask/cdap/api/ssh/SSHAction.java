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
    private static final boolean AUTHENTICATE_WITH_PASSWORD = false;

    private static final String HOST = "realtimekvtablesinktest10391-1000.dev.continuuity.net";
    private static final int PORT = 22;
    private static final String USER = "kashif";
    private static final String PASSWORD = "";

    private static final String PRIVATE_KEY_FILE = "/Users/Kashif/.ssh/backup/id_rsa";
    private static final String PRIVATE_KEY_PASSPHRASE = "tjrnuj8k";
    private static final String KNOWN_HOSTS_FILE = "/Users/Kashif/.ssh/known_hosts";

    private static final String CMD = "uptime";
  }

  private static final Logger LOG = LoggerFactory.getLogger(SSHAction.class);

  private boolean establishedConnection = false;
  private String stdout = null;
  private String stderr = null;

  @Override
  public void run() {
    try {
      Connection connection = new Connection(SSHConfig.HOST);
      connection.connect();

      // Authenticate with password or private key file
      if (SSHConfig.AUTHENTICATE_WITH_PASSWORD) {
        establishedConnection = connection.authenticateWithPassword(SSHConfig.USER, SSHConfig.PASSWORD);
      } else {
        File privateKey = new File(SSHConfig.PRIVATE_KEY_FILE);
        establishedConnection = connection.authenticateWithPublicKey(SSHConfig.USER, privateKey,
                                                                     SSHConfig.PRIVATE_KEY_PASSPHRASE);
      }

      if (!establishedConnection()) {
        throw new IOException(String.format("Unable to establish SSH connection for %s@%s on port %d",
                                            SSHConfig.USER, SSHConfig.HOST, SSHConfig.PORT));
      }

      Session session = connection.openSession();
      session.execCommand(SSHConfig.CMD);

      // Read stdout and stderr
      InputStream stdout = new StreamGobbler(session.getStdout());
      BufferedReader outBuffer = new BufferedReader(new InputStreamReader(stdout));
      StringBuilder outBuilder = new StringBuilder();
      InputStream stderr = new StreamGobbler(session.getStderr());
      BufferedReader errBuffer = new BufferedReader(new InputStreamReader(stderr));
      StringBuilder errBuilder = new StringBuilder();

      String line = outBuffer.readLine();
      while(line != null) {
        outBuilder.append(line + "\n");
        line = outBuffer.readLine();
      }

      line = errBuffer.readLine();
      while (line != null) {
        errBuilder.append(line + "\n");
        line = errBuffer.readLine();
      }

      LOG.info("Output:");
      LOG.info(outBuilder.toString());
      LOG.info("Errors:");
      LOG.info(errBuilder.toString());

      session.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public boolean establishedConnection() {
    return establishedConnection;
  }
}
