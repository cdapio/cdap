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

  protected void setPreferences(String[] programIdParts, PrintStream printStream,
                                Map<String, String> args) throws Exception {
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
        client.setApplicationPreferences(parseAppId(programIdParts), args);
        printSuccessMessage(printStream, type);
        break;

      case FLOW:
      case MAPREDUCE:
      case WORKFLOW:
      case SERVICE:
      case SPARK:
        client.setProgramPreferences(parseProgramId(programIdParts, type.getProgramType()), args);
        printSuccessMessage(printStream, type);
        break;

      default:
        throw new IllegalArgumentException("Unrecognized element type for preferences " + type.getTitleName());
    }
  }

  protected String determinePattern(String action) {

    switch (action) {
      case "set":
        return determinePatternSetHelper();
      case "load":
        return determinePatternLoadHelper();
    }
    return "None";
  }

  private String determinePatternSetHelper() {
    switch (type) {
      case INSTANCE:
      case NAMESPACE:
        return String.format("set preferences %s <%s>",
                             type.getName(), ArgumentName.RUNTIME_ARGS);
      case APP:
      case FLOW:
      case MAPREDUCE:
      case WORKFLOW:
      case SERVICE:
      case SPARK:
        return String.format("set preferences %s <%s> <%s>", type.getName(), ArgumentName.RUNTIME_ARGS,
                             type.getArgumentName());
    }
    return "None";
  }

  private String determinePatternLoadHelper() {
    switch (type) {
      case INSTANCE:
      case NAMESPACE:
        return String.format("load preferences %s <%s> <%s>", type.getName(),
                             ArgumentName.LOCAL_FILE_PATH, ArgumentName.CONTENT_TYPE);
      case APP:
      case FLOW:
      case MAPREDUCE:
      case WORKFLOW:
      case SERVICE:
      case SPARK:
        return String.format("load preferences %s <%s> <%s> <%s>", type.getName(),
                             ArgumentName.LOCAL_FILE_PATH, ArgumentName.CONTENT_TYPE,
                             type.getArgumentName());
    }
    return "None";
  }
}
