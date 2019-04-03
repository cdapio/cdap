/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.exception.CommandInputError;
import io.cdap.cdap.cli.util.ArgumentParser;
import io.cdap.cdap.client.PreferencesClient;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.Map;

/**
 * Sets preferences for instance, namespace, application, program.
 */
public class SetPreferencesCommand extends AbstractSetPreferencesCommand {
  protected static final String SUCCESS = "Set preferences successfully for the '%s'";
  private final ElementType type;

  protected SetPreferencesCommand(ElementType type, PreferencesClient client, CLIConfig cliConfig) {
    super(type, client, cliConfig);
    this.type = type;
  }

  @Override
  public void printSuccessMessage(PrintStream printStream, ElementType type) {
    printStream.printf(SUCCESS + "\n", type.getName());
  }

  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String[] programIdParts = new String[0];
    // If program ID and preferences are in the wrong order, this will return invalid command format instead of
    // invalid preferences format
    if (arguments.hasArgument(type.getArgumentName().toString())) {
      programIdParts = arguments.get(type.getArgumentName().toString()).split("\\.");
      if (programIdParts.length < 2) {
        throw new CommandInputError(this);
      }
    }
    String preferences = arguments.get(ArgumentName.PREFERENCES.toString());
    Map<String, String> args = ArgumentParser.parseMap(preferences, ArgumentName.PREFERENCES.toString());
    setPreferences(arguments, printStream, args, programIdParts);
  }

  @Override
  public String getPattern() {
    return determinePattern("set");
  }

  @Override
  public String getDescription() {
    return String.format("Sets the preferences of %s. '<%s>' is specified in the format 'key1=v1 key2=v2'.",
                         Fragment.of(Article.A, type.getName()), ArgumentName.PREFERENCES);
  }
}
