/*
 * Copyright © 2012-2014 Cask Data, Inc.
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
import co.cask.cdap.cli.Constants;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.common.base.Supplier;
import com.googlecode.concurrenttrees.common.Iterables;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Prints helper text for all commands.
 */
public class HelpCommand implements Command {

  private final Supplier<Iterable<Command>> commands;
  private final CLIConfig config;

  public HelpCommand(Supplier<Iterable<Command>> commands, CLIConfig config) {
    this.commands = commands;
    this.config = config;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    output.println("CLI version " + config.getVersion());
    output.println(Constants.EV_HOSTNAME + "=" + config.getHost());
    output.println("Available commands:");
    List<Command> commandList = Iterables.toList(commands.get());
    Collections.sort(commandList, new Comparator<Command>() {
      @Override
      public int compare(Command command, Command command2) {
        return command.getPattern().compareTo(command2.getPattern());
      }
    });
    for (Command command : commandList) {
      output.printf("%s: %s\n", command.getPattern(), command.getDescription());
    }
  }

  @Override
  public String getPattern() {
    return "help";
  }

  @Override
  public String getDescription() {
    return "Prints this helper text";
  }
}
