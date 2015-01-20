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

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.client.PreferencesClient;

/**
 * Gets preferences for instance, namespace, application, program.
 */
public class GetPreferencesCommand extends AbstractGetPreferencesCommand {
  private final ElementType type;

  protected GetPreferencesCommand(ElementType type, PreferencesClient client, CLIConfig cliConfig) {
    super(type, client, cliConfig, false);
    this.type = type;
  }

  @Override
  public String getPattern() {
    return String.format("get %s preferences [<%s>]", type.getName(), type.getArgumentName());
  }

  @Override
  public String getDescription() {
    return "Gets the preferences of a " + type.getPrettyName();
  }
}
