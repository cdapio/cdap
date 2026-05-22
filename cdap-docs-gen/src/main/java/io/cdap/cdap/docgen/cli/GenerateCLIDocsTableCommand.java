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

package io.cdap.cdap.docgen.cli;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.cdap.cli.command.system.HelpCommand;
import io.cdap.cdap.cli.util.table.TableRendererConfig;
import io.cdap.common.cli.Arguments;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Generates data for the table in cdap-docs/reference-manual/source/cli-api.rst.
 */
public class GenerateCLIDocsTableCommand extends HelpCommand {

  public GenerateCLIDocsTableCommand(CommandSet commands, TableRendererConfig tableRendererConfig) {
    super(() -> Collections.singletonList(commands), tableRendererConfig);
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    Map<String, List<Command>> categorizedCommands = categorizeCommands(
        commands.get(), CommandCategory.GENERAL, c -> true);
    for (CommandCategory category : CommandCategory.values()) {
      output.printf("   **%s**\n", category.getOriginalName());
      List<Command> commandList = new ArrayList<>(
          categorizedCommands.getOrDefault(category.getName(), Collections.emptyList()));
      Collections.sort(commandList, new Comparator<Command>() {
        @Override
        public int compare(Command command, Command command2) {
          return command.getPattern().compareTo(command2.getPattern());
        }
      });
      for (Command command : commandList) {
        output.printf("   ``%s``,\"%s\"\n", command.getPattern(),
            command.getDescription().replace("\"", "\"\""));
      }
    }
  }

  private String simpleTitleCase(String sentence) {
    Iterator<String> transformedWords = Iterators.transform(
        Splitter.on(" ").split(sentence).iterator(), new Function<String, String>() {
          @Nullable
          @Override
          public String apply(@Nullable String input) {
            if (input == null) {
              return null;
            } else if (input.length() <= 1) {
              return input.toUpperCase();
            } else {
              return input.substring(0, 1).toUpperCase() + input.substring(1).toLowerCase();
            }
          }
        });
    return Joiner.on(" ").join(transformedWords);
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
