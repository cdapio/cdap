/*
 * Copyright 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command;

import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.CLIConfig;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Prints the version.
 */
public class VersionCommand extends AbstractCommand {

  private final CLIConfig cliConfig;

  @Inject
  public VersionCommand(CLIConfig cliConfig) {
    super("version", null, "Prints the version");
    this.cliConfig = cliConfig;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    output.println(cliConfig.getVersion());
  }
}
