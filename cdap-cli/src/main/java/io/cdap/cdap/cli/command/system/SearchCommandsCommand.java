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

package io.cdap.cdap.cli.command.system;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.cdap.cli.util.table.TableRendererConfig;
import io.cdap.common.cli.Arguments;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

import java.io.PrintStream;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Searches available commands.
 */
public class SearchCommandsCommand extends HelpCommand {

  private final Supplier<Iterable<CommandSet<Command>>> commands;

  public SearchCommandsCommand(Supplier<Iterable<CommandSet<Command>>> commands,
                               TableRendererConfig tableRendererConfig) {
    super(commands, tableRendererConfig);
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
    return String.format("cli search commands <%s>", ArgumentName.QUERY);
  }

  @Override
  public String getDescription() {
    return "Searches available commands using regex";
  }
}
