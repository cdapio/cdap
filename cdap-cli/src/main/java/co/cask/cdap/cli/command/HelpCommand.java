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
import com.google.common.base.Strings;
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

  private static final int COL_WIDTH = 80;

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
    output.println();
    output.println("Available commands:");
    List<Command> commandList = Iterables.toList(commands.get());
    Collections.sort(commandList, new Comparator<Command>() {
      @Override
      public int compare(Command command, Command command2) {
        return command.getPattern().compareTo(command2.getPattern());
      }
    });
    for (Command command : commandList) {
      printPattern(command.getPattern(), output, COL_WIDTH);
      wrappedPrint(command.getDescription(), output, COL_WIDTH, 2);
      output.println();
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
   */
  private void printPattern(String pattern, PrintStream output, int colWidth) {
    if (pattern.length() <= colWidth) {
      output.println(pattern);
      return;
    }

    // Find the first <argument>, it is used for alignment in second line and onward.
    int startIdx = pattern.indexOf('<');
    // If no '<', it shouldn't reach here (it should be a short command). If it does, just print it.
    if (startIdx < 0) {
      output.println(pattern);
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
    output.println(pattern.substring(0, idx));

    if ((idx + 1) < pattern.length()) {
      // For rest of the line, align them to the startIdx
      wrappedPrint(pattern.substring(idx + 1), output, colWidth, startIdx);
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
