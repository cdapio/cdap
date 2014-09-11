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
import co.cask.cdap.shell.CommandSet;
import co.cask.cdap.shell.Constants;
import com.google.common.base.Supplier;

import java.io.PrintStream;

/**
 * Prints helper text for all commands.
 */
public class HelpCommand extends AbstractCommand {

  private final Supplier<CommandSet> getCommands;
  private final CLIConfig config;

  public HelpCommand(Supplier<CommandSet> getCommands, CLIConfig config) {
    super("help", null, "Prints this helper text");
    this.getCommands = getCommands;
    this.config = config;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    output.println("CLI version " + config.getVersion());
    output.println(Constants.EV_HOSTNAME + "=" + config.getHost());
    output.println();
    output.println("Available commands: \n" + getCommands.get().getHelperText(""));
  }
}
