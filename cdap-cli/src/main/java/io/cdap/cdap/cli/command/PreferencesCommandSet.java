/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.cli.command;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.Categorized;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.client.PreferencesClient;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

import java.util.List;

/**
 * Contains commands to get Preferences.
 */
public class PreferencesCommandSet extends CommandSet<Command> implements Categorized {

  @Inject
  public PreferencesCommandSet(PreferencesClient client, CLIConfig cliConfig) {
    super(generateCommands(client, cliConfig));
  }

  private static List<Command> generateCommands(PreferencesClient client, CLIConfig cliConfig) {
    List<Command> commands = Lists.newArrayList();
    for (ElementType elementType : ElementType.values()) {
      if (elementType.hasPreferences()) {
        commands.add(new GetPreferencesCommand(elementType, client, cliConfig));
        commands.add(new GetResolvedPreferencesCommand(elementType, client, cliConfig));
        commands.add(new SetPreferencesCommand(elementType, client, cliConfig));
        commands.add(new DeletePreferencesCommand(elementType, client, cliConfig));
        commands.add(new LoadPreferencesCommand(elementType, client, cliConfig));
      }
    }
    return commands;
  }

  @Override
  public String getCategory() {
    return CommandCategory.APPLICATION_LIFECYCLE.getName();
  }
}
