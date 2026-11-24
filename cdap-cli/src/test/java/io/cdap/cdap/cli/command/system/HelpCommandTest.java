/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.Categorized;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.common.cli.Arguments;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link HelpCommand}.
 */
public class HelpCommandTest {

  @Test
  public void testCategoryFallbackHierarchyAndExecute() throws Exception {
    Command directCategorized = new CategorizedTestCommand(
        "cmd-direct", "Direct category", CommandCategory.ARTIFACT.getName());
    Command inheritedFromSet = new PlainTestCommand("cmd-inherited", "Inherited category");
    Command nestedInherited = new PlainTestCommand("cmd-nested", "Nested inherited category");
    Command defaultFallback = new PlainTestCommand("cmd-default", "Default category");

    CommandSet<Command> uncategorizedNestedSet =
        new CommandSet<>(Collections.singletonList(nestedInherited));
    CommandSet<Command> categorizedParentSet = new CategorizedCommandSet(
        ImmutableList.of(directCategorized, inheritedFromSet),
        Collections.singletonList(uncategorizedNestedSet),
        CommandCategory.DATASET.getName());
    CommandSet<Command> uncategorizedTopSet =
        new CommandSet<>(Collections.singletonList(defaultFallback));

    Iterable<CommandSet<Command>> commandSets =
        ImmutableList.of(categorizedParentSet, uncategorizedTopSet);

    HelpCommand helpCommand = new HelpCommand(() -> commandSets, () -> 80);
    Multimap<String, Command> categorized = helpCommand.categorizeCommands(
        commandSets, CommandCategory.GENERAL, Predicates.alwaysTrue());

    Assert.assertTrue(
        categorized.get(CommandCategory.ARTIFACT.getName()).contains(directCategorized));
    Assert.assertFalse(
        categorized.get(CommandCategory.DATASET.getName()).contains(directCategorized));
    Assert.assertTrue(
        categorized.get(CommandCategory.DATASET.getName()).contains(inheritedFromSet));
    Assert.assertTrue(
        categorized.get(CommandCategory.DATASET.getName()).contains(nestedInherited));
    Assert.assertTrue(
        categorized.get(CommandCategory.GENERAL.getName()).contains(defaultFallback));

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (PrintStream ps = new PrintStream(buffer, true, "UTF-8")) {
      helpCommand.execute(new Arguments(Collections.emptyMap(), ""), ps);
    }
    String allHelpOutput = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    Assert.assertTrue(allHelpOutput.contains("cmd-direct"));
    Assert.assertTrue(allHelpOutput.contains("cmd-inherited"));
    Assert.assertTrue(allHelpOutput.contains("cmd-nested"));
    Assert.assertTrue(allHelpOutput.contains("cmd-default"));

    ByteArrayOutputStream filteredBuffer = new ByteArrayOutputStream();
    try (PrintStream ps = new PrintStream(filteredBuffer, true, "UTF-8")) {
      helpCommand.execute(new Arguments(
          Collections.singletonMap(
              ArgumentName.COMMAND_CATEGORY.getName(), CommandCategory.DATASET.getName()),
          ""), ps);
    }
    String datasetHelpOutput = new String(filteredBuffer.toByteArray(), StandardCharsets.UTF_8);
    Assert.assertTrue(datasetHelpOutput.contains("cmd-inherited"));
    Assert.assertTrue(datasetHelpOutput.contains("cmd-nested"));
    Assert.assertFalse(datasetHelpOutput.contains("cmd-direct"));
  }

  private static class PlainTestCommand implements Command {
    private final String pattern;
    private final String description;

    PlainTestCommand(String pattern, String description) {
      this.pattern = pattern;
      this.description = description;
    }

    @Override
    public void execute(Arguments arguments, PrintStream output) {
    }

    @Override
    public String getPattern() {
      return pattern;
    }

    @Override
    public String getDescription() {
      return description;
    }
  }

  private static final class CategorizedTestCommand extends PlainTestCommand implements Categorized {
    private final String category;

    CategorizedTestCommand(String pattern, String description, String category) {
      super(pattern, description);
      this.category = category;
    }

    @Override
    public String getCategory() {
      return category;
    }
  }

  private static final class CategorizedCommandSet extends CommandSet<Command>
      implements Categorized {
    private final String category;

    CategorizedCommandSet(
        Iterable<Command> commands, Iterable<CommandSet<Command>> commandSets, String category) {
      super(commands, commandSets);
      this.category = category;
    }

    @Override
    public String getCategory() {
      return category;
    }
  }
}
