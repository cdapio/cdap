/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.shell.command2.common;

import co.cask.cdap.shell.CLIConfig;
import co.cask.cdap.shell.command2.Arguments;
import co.cask.cdap.shell.command2.Command;
import co.cask.cdap.shell.util.SocketUtil;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Connects to a CDAP instance.
 */
public class ConnectCommand implements Command {

  private final CLIConfig config;

  @Inject
  public ConnectCommand(CLIConfig config) {
    this.config = config;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String hostname = arguments.get("hostname");
    int port = config.getClientConfig().getPort();

    if (!SocketUtil.isAvailable(hostname, port)) {
      throw new IOException(String.format("Host %s on port %d could not be reached", hostname, port));
    }

    config.setHostname(hostname);
    output.printf("Successfully connected CDAP host at %s:%d\n", hostname, port);
  }

  @Override
  public String getPattern() {
    return "connect <hostname>";
  }

  @Override
  public String getDescription() {
    return "Connects to a CDAP instance";
  }
}
