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
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.exception.UnAuthorizedAccessTokenException;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

/**
 * Abstract Class for getting preferences for instance, namespace, application, program.
 */
public abstract class AbstractGetPreferencesCommand extends AbstractCommand {
  private final PreferencesClient client;
  private final ElementType type;
  private final boolean resolved;
  private final CLIConfig cliConfig;

  protected AbstractGetPreferencesCommand(ElementType type, PreferencesClient client, CLIConfig cliConfig,
                                          boolean resolved) {
    super(cliConfig);
    this.type = type;
    this.client = client;
    this.resolved = resolved;
    this.cliConfig = cliConfig;
  }

  private String joinMapEntries(Map<String, String> map) {
    return Joiner.on(String.format("%n")).join(map.entrySet().iterator());
  }

  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String[] programIdParts = new String[0];
    if (arguments.hasArgument(type.getArgumentName().toString())) {
      programIdParts = arguments.get(type.getArgumentName().toString()).split("\\.");
    }

    printStream.print(joinMapEntries(parsePreferences(programIdParts)));
  }

  private String getCurrentNamespaceId() {
    return cliConfig.getCurrentNamespace().getId();
  }

  private Map<String, String> parsePreferences(String[] programIdParts)
    throws IOException, UnAuthorizedAccessTokenException, NotFoundException {

    switch(type) {
      case INSTANCE:
        checkInputLength(programIdParts, 0);
        return client.getInstancePreferences();
      case NAMESPACE:
        checkInputLength(programIdParts, 0);
        return client.getNamespacePreferences(getCurrentNamespaceId(), resolved);
      case APP:
        checkInputLength(programIdParts, 1);
        return client.getApplicationPreferences(getCurrentNamespaceId(), programIdParts[0], resolved);
      case FLOW:
        checkInputLength(programIdParts, 2);
        return client.getProgramPreferences(getCurrentNamespaceId(),
                                            programIdParts[0], type.getProgramType(),
                                            programIdParts[1], resolved);

      case PROCEDURE:
        checkInputLength(programIdParts, 2);
        return client.getProgramPreferences(getCurrentNamespaceId(),
                                            programIdParts[0], type.getProgramType(),
                                            programIdParts[1], resolved);
      case MAPREDUCE:
        checkInputLength(programIdParts, 2);
        return client.getProgramPreferences(getCurrentNamespaceId(),
                                            programIdParts[0], type.getProgramType(),
                                            programIdParts[1], resolved);

      case WORKFLOW:
        checkInputLength(programIdParts, 2);
        return client.getProgramPreferences(getCurrentNamespaceId(),
                                            programIdParts[0], type.getProgramType(),
                                            programIdParts[1], resolved);
      case SERVICE:
        checkInputLength(programIdParts, 2);
        return client.getProgramPreferences(getCurrentNamespaceId(),
                                            programIdParts[0], type.getProgramType(),
                                            programIdParts[1], resolved);
      case SPARK:
        checkInputLength(programIdParts, 2);
        return client.getProgramPreferences(getCurrentNamespaceId(),
                                            programIdParts[0], type.getProgramType(),
                                            programIdParts[1], resolved);
      default:
        throw new IllegalArgumentException("Unrecognized Element Type for Preferences "  + type.getPrettyName());
    }
  }
}
