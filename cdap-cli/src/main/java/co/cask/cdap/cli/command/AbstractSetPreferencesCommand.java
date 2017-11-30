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
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.client.PreferencesClient;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;
import java.util.Map;

/**
 * Abstract Set Preferences Class.
 */
public abstract class AbstractSetPreferencesCommand extends AbstractCommand {

  private final PreferencesClient client;
  private final ElementType type;

  protected AbstractSetPreferencesCommand(ElementType type, PreferencesClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.type = type;
    this.client = client;
  }

  protected abstract void printSuccessMessage(PrintStream printStream, ElementType type);

  protected void setPreferences(Arguments arguments, PrintStream printStream,
                                Map<String, String> args) throws Exception {
    String[] programIdParts = new String[0];
    if (arguments.hasArgument(type.getArgumentName().toString())) {
      programIdParts = arguments.get(type.getArgumentName().toString()).split("\\.");
    }
    setPreferences(arguments, printStream, args, programIdParts);
  }

  protected void setPreferences(Arguments arguments, PrintStream printStream,
                                Map<String, String> args, String[] programIdParts) throws Exception {
    switch (type) {
      case INSTANCE:
        checkInputLength(programIdParts, 0);
        client.setInstancePreferences(args);
        printSuccessMessage(printStream, type);
        break;

      case NAMESPACE:
        checkInputLength(programIdParts, 0);
        client.setNamespacePreferences(cliConfig.getCurrentNamespace(), args);
        printSuccessMessage(printStream, type);
        break;

      case APP:
        client.setApplicationPreferences(parseApplicationId(arguments), args);
        printSuccessMessage(printStream, type);
        break;

      case FLOW:
      case MAPREDUCE:
      case WORKFLOW:
      case SERVICE:
      case SPARK:
        client.setProgramPreferences(parseProgramId(arguments, type), args);
        printSuccessMessage(printStream, type);
        break;

      default:
        throw new IllegalArgumentException("Unrecognized element type for preferences: " + type.getShortName());
    }
  }

  protected String determinePattern(String action) {

    switch (action) {
      case "set":
        return determineSetPatternHelper();
      case "load":
        return determineLoadPatternHelper();
    }
    throw new RuntimeException("Unrecognized action: " + action);
  }

  private String determineSetPatternHelper() {
    switch (type) {
      case INSTANCE:
      case NAMESPACE:
        return String.format("set %s preferences <%s>",
                             type.getShortName(), ArgumentName.PREFERENCES);
      case APP:
      case FLOW:
      case MAPREDUCE:
      case WORKFLOW:
      case SERVICE:
      case WORKER:
      case SPARK:
        return String.format("set %s preferences <%s> <%s>", type.getShortName(), type.getArgumentName(),
                             ArgumentName.PREFERENCES);
    }
    throw new RuntimeException("Unrecognized element type: " + type.getShortName());
  }

  private String determineLoadPatternHelper() {
    switch (type) {
      case INSTANCE:
      case NAMESPACE:
        return String.format("load %s preferences <%s> <%s>", type.getShortName(),
                             ArgumentName.LOCAL_FILE_PATH, ArgumentName.CONTENT_TYPE);
      case APP:
      case FLOW:
      case MAPREDUCE:
      case WORKFLOW:
      case SERVICE:
      case WORKER:
      case SPARK:
        return String.format("load %s preferences <%s> <%s> <%s>", type.getShortName(), type.getArgumentName(),
                             ArgumentName.LOCAL_FILE_PATH, ArgumentName.CONTENT_TYPE);
    }
    throw new RuntimeException("Unrecognized element type: " + type.getShortName());
  }
}
