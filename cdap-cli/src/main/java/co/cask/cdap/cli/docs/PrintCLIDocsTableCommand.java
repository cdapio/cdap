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

import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Generates data for the table in cdap-docs/reference-manual/source/cli-api.rst.
 */
public class PrintCLIDocsTableCommand implements Command {

  private final Iterable<Command> commands;

  public PrintCLIDocsTableCommand(Iterable<Command> commands) {
    this.commands = commands;
  }

  private enum Category {
    GENERAL("General", ImmutableSet.of("")),
    CALL_AND_CONNECT("Calling and Connecting", ImmutableSet.of("call", "connect")),
    CREATE("Creating", ImmutableSet.of("create")),
    DELETE("Deleting", ImmutableSet.of("delete")),
    DEPLOY("Deploying", ImmutableSet.of("deploy")),
    DESCRIBE("Describing", ImmutableSet.of("describe")),
    EXECUTE("Executing Queries", ImmutableSet.of("execute")),
    GET("Retrieving Information", ImmutableSet.of("get")),
    LIST("Listing Elements", ImmutableSet.of("list")),
    SEND("Sending Events", ImmutableSet.of("send")),
    SET("Setting", ImmutableSet.of("set")),
    START("Starting", ImmutableSet.of("start")),
    STOP("Stopping", ImmutableSet.of("stop")),
    TRUNCATE("Truncating", ImmutableSet.of("truncate"));

    private final String name;
    private final ImmutableSet<String> prefixes;

    Category(String name, ImmutableSet<String> prefixes) {
      this.name = name;
      this.prefixes = prefixes;
    }

    public String getName() {
      return name;
    }

    public ImmutableSet<String> getPrefixes() {
      return prefixes;
    }
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    List<Command> commandList = com.googlecode.concurrenttrees.common.Iterables.toList(commands);
    Multimap<Category, Command> categorizedCommands = categorizeCommands(commandList);

    for (Category category : Category.values()) {
      List<Command> commandsInCategory = Lists.newArrayList(
        Optional.fromNullable(categorizedCommands.get(category)).or(ImmutableList.<Command>of()));

      output.printf("   **%s**\n", category.getName());
      Collections.sort(commandsInCategory, new Comparator<Command>() {
        @Override
        public int compare(Command o, Command o2) {
          return o.getPattern().compareTo(o2.getPattern());
        }
      });
      for (Command command : commandsInCategory) {
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

  private Multimap<Category, Command> categorizeCommands(List<Command> commandList) {
    Multimap<Category, Command> result = LinkedListMultimap.create();

    Map<String, Category> prefixToCategoryMap = Maps.newHashMap();
    for (Category category : Category.values()) {
      for (String prefix : category.getPrefixes()) {
        prefixToCategoryMap.put(prefix, category);
      }
    }

    for (Command command : commandList) {
      String patternFirstWord = command.getPattern().split(" ")[0];
      if (prefixToCategoryMap.containsKey(patternFirstWord)) {
        Category category = prefixToCategoryMap.get(patternFirstWord);
        result.put(category, command);
      } else {
        result.put(Category.GENERAL, command);
      }
    }

    return result;
  }

}
