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
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.Constants;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Prints helper text for all commands.
 */
public class HelpCommand implements Command {

  private static final int COL_WIDTH = 80;

  private final Supplier<Iterable<CommandSet<Command>>> commands;
  private final CLIConfig config;

  public HelpCommand(Supplier<Iterable<CommandSet<Command>>> commands, CLIConfig config) {
    this.commands = commands;
    this.config = config;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    output.println();

    if (arguments.hasArgument(ArgumentName.COMMAND_CATEGORY.getName())) {
      // help with one category
      Multimap<String, Command> categorizedCommands = categorizeCommands(commands.get(), CommandCategory.GENERAL);
      String commandCategoryInput = arguments.get(ArgumentName.COMMAND_CATEGORY.getName());
      CommandCategory category = CommandCategory.valueOfNameIgnoreCase(commandCategoryInput);

      List<Command> commandList = Lists.newArrayList(categorizedCommands.get(category.getName()));
      if (commandList.isEmpty()) {
        output.printf("No commands found in the %s category", category.getName());
        output.println();
        return;
      }

      printCommands(output, category.getName(), commandList);
    } else {
      // normal help
      output.println("CLI version " + config.getVersion());
      output.println(Constants.EV_HOSTNAME + "=" + config.getHost());
      output.println();
      output.println("Available commands:");
      output.println();

      Multimap<String, Command> categorizedCommands = categorizeCommands(commands.get(), CommandCategory.GENERAL);
      for (CommandCategory category : CommandCategory.values()) {
        List<Command> commandList = Lists.newArrayList(categorizedCommands.get(category.getName()));
        if (commandList.isEmpty()) {
          continue;
        }

        printCommands(output, category.getName(), commandList);
      }
    }
  }

  private void printCommands(PrintStream output, String category, List<Command> commandList) {
    Collections.sort(commandList, new Comparator<Command>() {
      @Override
      public int compare(Command command, Command command2) {
        return command.getPattern().compareTo(command2.getPattern());
      }
    });

    output.println(category);
    output.println();

    for (Command command : commandList) {
      printPattern(command.getPattern(), output, COL_WIDTH, 2);
      wrappedPrint(command.getDescription(), output, COL_WIDTH, 4);
      output.println();
    }
  }

  private Multimap<String, Command> categorizeCommands(Iterable<CommandSet<Command>> commandSets,
                                                       CommandCategory defaultCategory) {
    Multimap<String, Command> result = HashMultimap.create();
    for (CommandSet<Command> commandSet : commandSets) {
      populate(result, commandSet, getCategory(commandSet), defaultCategory);
    }
    return result;
  }

  /**
   * Recursive helper for {@link #categorizeCommands(Iterable, CommandCategory)}.
   */
  private void populate(Multimap<String, Command> result, CommandSet<Command> commandSet,
                        Optional<String> parentCategory, CommandCategory defaultCategory) {

    for (Command childCommand : commandSet.getCommands()) {
      Optional<String> commandCategory = getCategory(childCommand).or(parentCategory);
      result.put(commandCategory.or(defaultCategory.getName()), childCommand);
    }

    for (CommandSet<Command> childCommandSet : commandSet.getCommandSets()) {
      Optional<String> commandCategory = getCategory(childCommandSet).or(parentCategory);
      populate(result, childCommandSet, commandCategory, defaultCategory);
    }
  }

  private Optional<String> getCategory(Object object) {
    if (object instanceof Categorized) {
      return Optional.of(((Categorized) object).getCategory());
    }
    return Optional.absent();
  }

  @Override
  public String getPattern() {
    return String.format("help [<%s>]", ArgumentName.COMMAND_CATEGORY);
  }

  @Override
  public String getDescription() {
    return String.format("Prints this helper text. Optionally provide <%s> to get help with a specific category",
                         ArgumentName.COMMAND_CATEGORY);
  }

  /**
   * Prints the given command pattern with text wrapping at the given column width. It prints multiple lines as:
   *
   * <pre>{@code
   * command sub-command <arg1> <arg2>
   *                     <arg3> [<optional-long-arg4>]
   * }
   * </pre>
   *
   * @param pattern the command pattern to print
   * @param output the {@link PrintStream} to write to
   * @param colWidth width of the column
   * @param prefixSpaces number of spaces as the prefix for each line printed
   */
  private void printPattern(String pattern, PrintStream output, int colWidth, int prefixSpaces) {
    String prefix = Strings.repeat(" ", prefixSpaces);
    colWidth -= prefixSpaces;

    if (pattern.length() <= colWidth) {
      output.printf("%s%s", prefix, pattern);
      output.println();
      return;
    }

    // Find the first <argument>, it is used for alignment in second line and onward.
    int startIdx = pattern.indexOf('<');
    // If no '<', it shouldn't reach here (it should be a short command). If it does, just print it.
    if (startIdx < 0) {
      output.printf("%s%s", prefix, pattern);
      output.println();
      return;
    }

    // First line should at least include the first <argument>
    int idx = pattern.lastIndexOf('>', colWidth) + 1;
    if (idx <= 0) {
      idx = pattern.indexOf('>', startIdx) + 1;
      if (idx <= 0) {
        idx = pattern.length();
      }
    }
    // Make sure we include the closing ] of an optional argument
    if (idx < pattern.length() && pattern.charAt(idx) == ']') {
      idx++;
    }
    output.printf("%s%s", prefix, pattern.substring(0, idx));
    output.println();

    if ((idx + 1) < pattern.length()) {
      // For rest of the line, align them to the startIdx
      wrappedPrint(pattern.substring(idx + 1), output, colWidth, startIdx + prefixSpaces);
    }
  }

  /**
   * Prints the given string with text wrapping at the given column width.
   *
   * @param str the string to print
   * @param output the {@link PrintStream} to write to
   * @param colWidth width of the column
   * @param prefixSpaces number of spaces as the prefix for each line printed
   */
  private void wrappedPrint(String str, PrintStream output, int colWidth, int prefixSpaces) {
    String prefix = Strings.repeat(" ", prefixSpaces);
    colWidth -= prefixSpaces;

    if (str.length() <= colWidth) {
      output.printf("%s%s", prefix, str);
      output.println();
      return;
    }

    int beginIdx = 0;
    while (beginIdx < str.length()) {
      int idx;
      if (beginIdx + colWidth >= str.length()) {
        idx = str.length();
      } else {
        idx = str.lastIndexOf(' ', beginIdx + colWidth);
      }

      // Cannot break line if no space found between beginIdx and (beginIdx + colWidth)
      // The best we can do is to look forward.
      // The line will be longer than colWidth though.
      if (idx < 0 || idx < beginIdx) {
        idx = str.indexOf(' ', beginIdx + colWidth);
        if (idx < 0) {
          idx = str.length();
        }
      }
      output.printf("%s%s", prefix, str.substring(beginIdx, idx));
      beginIdx = idx + 1;
      output.println();
    }
  }
}
