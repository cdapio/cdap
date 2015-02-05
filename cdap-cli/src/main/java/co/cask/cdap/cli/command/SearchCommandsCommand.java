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

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.CommandCategory;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.io.PrintStream;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Searches available commands.
 */
public class SearchCommandsCommand extends HelpCommand {

  private final Supplier<Iterable<CommandSet<Command>>> commands;

  public SearchCommandsCommand(Supplier<Iterable<CommandSet<Command>>> commands, CLIConfig config) {
    super(commands, config);
    this.commands = commands;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String originalQuery = arguments.get(ArgumentName.QUERY.getName());
    String queryString = originalQuery;

    if (!queryString.startsWith("^")) {
      queryString = "^.*?" + queryString;
    }

    if (!queryString.endsWith("$")) {
      queryString = queryString + ".+?$";
    }

    final String query = queryString;
    Predicate<Command> filter = new Predicate<Command>() {
      @Override
      public boolean apply(@Nullable Command input) {
        return input != null && input.getPattern().matches(query);
      }
    };

    output.println();
    Multimap<String, Command> categorizedCommands = categorizeCommands(commands.get(), CommandCategory.GENERAL, filter);
    if (categorizedCommands.isEmpty()) {
      output.printf("No matches found for \"%s\"", originalQuery);
      output.println();
    } else {
      output.printf("Listing matches for \"%s\":", originalQuery);
      output.println();
      output.println();

      for (CommandCategory category : CommandCategory.values()) {
        List<Command> commandList = Lists.newArrayList(categorizedCommands.get(category.getName()));
        if (commandList.isEmpty()) {
          continue;
        }

        printCommands(output, category.getName(), commandList);
      }
    }
  }

  @Override
  public String getPattern() {
    return String.format("search commands <%s>", ArgumentName.QUERY);
  }

  @Override
  public String getDescription() {
    return "Searches available commands using regex";
  }
}
