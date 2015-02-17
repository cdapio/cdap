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

package co.cask.cdap.cli.docs;

import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.command.HelpCommand;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Generates data for the table in cdap-docs/reference-manual/source/cli-api.rst.
 */
public class GenerateCLIDocsTableCommand extends HelpCommand {

  public GenerateCLIDocsTableCommand(Supplier<Iterable<CommandSet<Command>>> commands) {
    super(commands);
  }


  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    Multimap<String, Command> categorizedCommands = categorizeCommands(
      commands.get(), CommandCategory.GENERAL, Predicates.<Command>alwaysTrue());
    for (String category : categorizedCommands.keySet()) {
      output.printf("   **%s**\n", category);
      List<Command> commandList = Lists.newArrayList(categorizedCommands.get(category));
      Collections.sort(commandList, new Comparator<Command>() {
        @Override
        public int compare(Command command, Command command2) {
          return command.getPattern().compareTo(command2.getPattern());
        }
      });
      for (Command command : commandList) {
        output.printf("   ``%s``,\"%s\"\n", command.getPattern(), command.getDescription().replace("\"", "\"\""));
      }
    }
  }

  @Override
  public String getPattern() {
    return "null";
  }

  @Override
  public String getDescription() {
    return "null";
  }
}
