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

package co.cask.cdap.common.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;

/**
 * Set of {@link Command}s.
 */
public class CommandSet<T extends Command> implements Iterable<T> {

  private final List<T> commands;
  private final List<CommandSet<T>> commandSets;

  public CommandSet(Iterable<T> commands, Iterable<CommandSet<T>> commandSets) {
    this.commands = ImmutableList.copyOf(commands);
    this.commandSets = ImmutableList.copyOf(commandSets);
  }

  public CommandSet(Iterable<T> commands) {
    this(commands, ImmutableList.<CommandSet<T>>of());
  }

  public Iterator<T> iterator() {
    return Iterables.concat(commands, Iterables.concat(commandSets)).iterator();
  }

  /**
   * Finds a matching command for the provided input.
   *
   * @param input the input string
   * @return the matching command and the parsed arguments
   */
  public CommandMatch findMatch(String input) {
    for (Command command : this) {
      String pattern = command.getPattern();

      if (pattern.matches(".*<\\S+>.*")) {
        // if pattern has an argument, check if input startsWith the pattern before first argument
        String patternPrefix = pattern.substring(0, pattern.indexOf(" <"));
        if (input.startsWith(patternPrefix)) {
          return new CommandMatch(command, input);
        }
      } else {
        // if pattern has no argument, the entire input must match
        if (input.equals(pattern)) {
          return new CommandMatch(command, input);
        }
      }
    }

    return null;
  }
}
