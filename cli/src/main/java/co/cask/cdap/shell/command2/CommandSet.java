/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.shell.command2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Set of {@link Command}s.
 */
public class CommandSet {

  private final List<Command> commands;

  public CommandSet(List<Command> commands) {
    this.commands = ImmutableList.copyOf(commands);
  }

  public CommandSet(Command... commands) {
    this.commands = ImmutableList.copyOf(commands);
  }

  public List<Command> getCommands() {
    return commands;
  }

  /**
   * Finds a matching command for the provided input.
   *
   * @param input the input string
   * @return the matching command and the parsed arguments
   */
  public CommandMatch findMatch(String input) {
    for (Command command : commands) {
      String pattern = command.getPattern();

      if (pattern.matches(".*<\\S+>.*")) {
        // if pattern has an argument, check if input startsWith the pattern before first argument
        String patternPrefix = pattern.substring(0, pattern.indexOf(" <"));
        if (input.startsWith(patternPrefix)) {
          return new CommandMatch(command, parseArguments(command, input));
        }
      } else {
        // if pattern has no argument, the entire input must match
        if (input.equals(pattern)) {
          return new CommandMatch(command, parseArguments(command, input));
        }
      }
    }

    return null;
  }

  private Arguments parseArguments(Command command, String input) {
    String pattern = command.getPattern();
    Map<Integer, String> argumentNames = getArgumentNames(command);

    String regexPatternString = pattern.replaceAll("<(\\S+)>", "(\\\\S+)");
    Pattern regexPattern = Pattern.compile(regexPatternString);
    Matcher matcher = regexPattern.matcher(input);

    if (!matcher.matches()) {
      throw new RuntimeException("Wrong arguments provided: " + pattern);
    }

    ImmutableMap.Builder<String, String> args = ImmutableMap.builder();
    for (int i = 1; i < matcher.groupCount() + 1; i++) {
      String argumentName = argumentNames.get(i);
      String argumentValue = matcher.group(i);
      args.put(argumentName, argumentValue);
    }

    return new Arguments(args.build());
  }

  private Map<Integer, String> getArgumentNames(Command command) {
    ImmutableMap.Builder<Integer, String> argumentNames = new ImmutableMap.Builder<Integer, String>();
    Pattern pattern = Pattern.compile(".*<(\\S+)>.*");
    Matcher matcher = pattern.matcher(command.getPattern());

    if (matcher.matches()) {
      for (int i = 1; i < matcher.groupCount() + 1; i++) {
        String match = matcher.group(i);
        argumentNames.put(i, match);
      }
    }

    return argumentNames.build();
  }

}
