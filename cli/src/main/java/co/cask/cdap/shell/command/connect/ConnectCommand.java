/*
 * Copyright 2012-2014 Cask, Inc.
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

package co.cask.cdap.shell.command.connect;

import co.cask.cdap.shell.CLIConfig;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.util.SocketUtil;

import java.io.IOException;
import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Connects to a CDAP instance.
 */
public class ConnectCommand extends AbstractCommand {

  private final CLIConfig cliConfig;

  @Inject
  public ConnectCommand(CLIConfig cliConfig) {
    super("connect", "<cdap-hostname>", "Connects to a CDAP instance");
    this.cliConfig = cliConfig;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String hostname = args[0];
    int port = cliConfig.getClientConfig().getPort();

    if (!SocketUtil.isAvailable(hostname, port)) {
      throw new IOException(String.format("Host %s on port %d could not be reached", hostname, port));
    }

    cliConfig.setHostname(hostname);
    output.printf("Successfully connected CDAP host at %s:%d\n", hostname, port);
  }
}
