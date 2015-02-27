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
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.client.PreferencesClient;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Deletes preferences for instance, namespace, application, program.
 */
public class DeletePreferencesCommand extends AbstractCommand {
  private static final String SUCCESS = "Deleted Preferences successfully for the '%s'";

  private final PreferencesClient client;
  private final ElementType type;
  private final CLIConfig cliConfig;

  protected DeletePreferencesCommand(ElementType type, PreferencesClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.type = type;
    this.client = client;
    this.cliConfig = cliConfig;
  }

  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String[] programIdParts = new String[0];

    if (arguments.hasArgument(type.getArgumentName().toString())) {
      programIdParts = arguments.get(type.getArgumentName().toString()).split("\\.");
    }

    switch (type) {
      case INSTANCE:
        checkInputLength(programIdParts, 0);
        client.deleteInstancePreferences();
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case NAMESPACE:
        checkInputLength(programIdParts, 0);
        client.deleteNamespacePreferences(getCurrentNamespaceId());
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case APP:
        checkInputLength(programIdParts, 1);
        client.deleteApplicationPreferences(getCurrentNamespaceId(), programIdParts[0]);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case FLOW:
      case PROCEDURE:
      case MAPREDUCE:
      case WORKFLOW:
      case SERVICE:
      case SPARK:
        checkInputLength(programIdParts, 2);
        client.deleteProgramPreferences(getCurrentNamespaceId(), programIdParts[0],
                                        type.getProgramType(), programIdParts[1]);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      default:
        throw new IllegalArgumentException("Unrecognized Element Type for Preferences " + type.getPrettyName());
    }
  }

  @Override
  public String getPattern() {
    return String.format("delete %s preferences [<%s>]", type.getName(), type.getArgumentName());
  }

  @Override
  public String getDescription() {
    return String.format("Deletes the preferences of a %s.", type.getPrettyName());
  }

  private String getCurrentNamespaceId() {
    return cliConfig.getCurrentNamespace().getId();
  }
}
