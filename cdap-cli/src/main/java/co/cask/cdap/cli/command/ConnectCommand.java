/*
 * Copyright © 2012-2015 Cask Data, Inc.
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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;

import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Connects to a CDAP instance.
 */
public class ConnectCommand implements Command {

  private final CLIConfig cliConfig;
  private final InstanceURIParser instanceURIParser;

  @Inject
  public ConnectCommand(CLIConfig cliConfig, InstanceURIParser instanceURIParser) {
    this.cliConfig = cliConfig;
    this.instanceURIParser = instanceURIParser;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String instanceURI = arguments.get("cdap-instance-uri");
    CLIConfig.ConnectionInfo connectionInfo = instanceURIParser.parse(instanceURI);
    try {
      cliConfig.tryConnect(connectionInfo, output, true);
    } catch (Exception e) {
      output.println("Failed to connect to " + instanceURI + ": " + e.getMessage());
    }
  }

  @Override
  public String getPattern() {
    return "connect <cdap-instance-uri>";
  }

  @Override
  public String getDescription() {
    return "Connects to a CDAP instance.";
  }
}
