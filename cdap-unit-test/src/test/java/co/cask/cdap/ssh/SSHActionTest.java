/*
 *
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

package co.cask.cdap.ssh;

import co.cask.cdap.api.ssh.SSHAction;
import org.junit.Test;

public class SSHActionTest {

  private static final boolean usePasswordSSH = false;
  private static final String host = "10.150.2.127";
  private static final int port = 22;
  private static final String user = "Kashif";
  private static final String password = "";
  private static final String privateKeyFile = "/Users/Kashif/.ssh/id_rsa";
  private static final String privateKeyPassphrase = "";
  private static final String cmd = "uptime";

  @Test
  public void testSSHAction() {
    SSHAction sshAction = new SSHAction(usePasswordSSH, host, port, user, password, privateKeyFile,
                                        privateKeyPassphrase, cmd);
    sshAction.run();
    assert(sshAction.establishedConnection());
  }
}
