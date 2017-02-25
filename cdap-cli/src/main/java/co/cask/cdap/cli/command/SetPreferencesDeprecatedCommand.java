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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.ArgumentParser;
import co.cask.cdap.client.PreferencesClient;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;
import java.util.Map;

/**
 * Sets preferences for instance, namespace, application, program.
 *
 * @deprecated since 4.1.0. Use {@link SetPreferencesCommand} instead.
 */
@Deprecated
public class SetPreferencesDeprecatedCommand extends AbstractSetPreferencesCommand {
  protected static final String SUCCESS = "Set preferences successfully for the '%s'";
  private final ElementType type;

  protected SetPreferencesDeprecatedCommand(ElementType type, PreferencesClient client, CLIConfig cliConfig) {
    super(type, client, cliConfig);
    this.type = type;
  }

  @Override
  public void printSuccessMessage(PrintStream printStream, ElementType type) {
    printStream.printf(SUCCESS + "\n", type.getName());
  }

  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String runtimeArgs = arguments.get(ArgumentName.RUNTIME_ARGS.toString());
    Map<String, String> args = ArgumentParser.parseMap(runtimeArgs, ArgumentName.RUNTIME_ARGS.toString());
    setPreferences(arguments, printStream, args);
  }

  @Override
  public String getPattern() {
    return determineDeprecatedPattern("set");
  }

  @Override
  public String getDescription() {
    return String.format("Sets the preferences of %s. '<%s>' is specified in the format 'key1=v1 key2=v2'. " +
                           "(Deprecated as of CDAP 4.1.0. Use %s instead)", Fragment.of(Article.A, type.getName()),
                         ArgumentName.RUNTIME_ARGS, determinePattern("set"));
  }
}
