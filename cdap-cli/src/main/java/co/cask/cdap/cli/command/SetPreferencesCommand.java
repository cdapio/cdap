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
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.PreferencesClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.common.cli.Arguments;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.io.PrintStream;
import java.util.Map;

/**
 * Sets preferences for instance, namespace, application, program.
 */
public class SetPreferencesCommand extends AbstractAuthCommand {
  private static final String SUCCESS = "Set Preferences successfully for the '%s'";
  private static final String JSON = "json";
  private static final Gson GSON = new Gson();
  private static final java.lang.reflect.Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final PreferencesClient client;
  private final ElementType type;

  protected SetPreferencesCommand(ElementType type, PreferencesClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.type = type;
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String[] programIdParts = new String[0];
    String jsonMap = arguments.get(JSON);

    if (arguments.hasArgument(type.getArgumentName().toString())) {
      programIdParts = arguments.get(type.getArgumentName().toString()).split("\\.");
    }

    Map<String, String> args;
    try {
      args = GSON.fromJson(jsonMap, STRING_MAP_TYPE);
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException("JSON format is invalid");
    }

    switch (type) {
      case INSTANCE:
        if (programIdParts.length != 0) {
          throw new CommandInputError(this);
        }
        client.setInstancePreferences(args);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case NAMESPACE:
        if (programIdParts.length != 1) {
          throw new CommandInputError(this);
        }
        client.setNamespacePreferences(programIdParts[0], args);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case APP:
        // todo : change this to 2 when namespace id is part of the argument.
        if (programIdParts.length != 1) {
          throw new CommandInputError(this);
        }
        client.setApplicationPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0], args);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case FLOW:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        client.setProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0], type.getPluralName(),
                                     programIdParts[1], args);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case PROCEDURE:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        client.setProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0], type.getPluralName(),
                                     programIdParts[1], args);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case MAPREDUCE:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        client.setProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0], type.getPluralName(),
                                     programIdParts[1], args);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case WORKFLOW:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        client.setProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0], type.getPluralName(),
                                     programIdParts[1], args);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case SERVICE:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        client.setProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0], type.getPluralName(),
                                     programIdParts[1], args);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      case SPARK:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        client.setProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0], type.getPluralName(),
                                     programIdParts[1], args);
        printStream.printf(SUCCESS + "\n", type.getPrettyName());
        break;

      default:
        throw new IllegalArgumentException("Unrecognized Element Type for Preferences " + type.getPrettyName());
    }
  }

  @Override
  public String getPattern() {
    return String.format("set %s preferences <%s> [<%s>]", type.getName(), JSON, type.getArgumentName());
  }

  @Override
  public String getDescription() {
    return "Sets the preferences of a " + type.getPluralPrettyName() + " using a json map";
  }
}
