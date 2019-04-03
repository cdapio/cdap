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

package io.cdap.cdap.cli.command;

import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.CLIConnectionConfig;
import io.cdap.cdap.cli.LaunchOptions;
import io.cdap.cdap.cli.util.InstanceURIParser;
import io.cdap.common.cli.Arguments;
import io.cdap.common.cli.Command;

import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Connects to a CDAP instance.
 */
public class ConnectCommand implements Command {

  private final CLIConfig cliConfig;
  private final InstanceURIParser instanceURIParser;
  private final boolean debug;

  @Inject
  public ConnectCommand(CLIConfig cliConfig, InstanceURIParser instanceURIParser,
                        LaunchOptions launchOptions) {
    this.cliConfig = cliConfig;
    this.instanceURIParser = instanceURIParser;
    this.debug = launchOptions.isDebug();
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String instanceURI = arguments.get(ArgumentName.INSTANCE_URI.toString());
    String verifySSLCertString = arguments.getOptional(ArgumentName.VERIFY_SSL_CERT.toString());
    boolean verifySSLCert = verifySSLCertString != null ? Boolean.valueOf(verifySSLCertString) : true;

    CLIConnectionConfig connection = instanceURIParser.parse(instanceURI);
    try {
      cliConfig.tryConnect(connection, verifySSLCert, output, debug);
    } catch (Exception e) {
      output.println("Failed to connect to " + instanceURI + ": " + e.getMessage());
      if (debug) {
        e.printStackTrace(output);
      }
    }
  }

  @Override
  public String getPattern() {
    return String.format("connect <%s> [<%s>]",
                         ArgumentName.INSTANCE_URI, ArgumentName.VERIFY_SSL_CERT);
  }

  @Override
  public String getDescription() {
    return "Connects to a CDAP instance";
  }
}
