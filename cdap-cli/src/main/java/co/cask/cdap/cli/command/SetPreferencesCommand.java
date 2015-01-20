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
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.PreferencesClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.common.cli.Arguments;
import com.google.common.base.Splitter;

import java.io.PrintStream;
import java.util.Map;

/**
 * Sets preferences for instance, namespace, application, program.
 */
public class SetPreferencesCommand extends AbstractAuthCommand {
  private static final String SUCCESS = "Set Preferences successfully for the '%s'";

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
    String runtimeArgs = arguments.get(ArgumentName.RUNTIME_ARGS.toString());
    Map<String, String> args = Splitter.on(" ").withKeyValueSeparator("=").split(runtimeArgs);

    if (arguments.hasArgument(type.getArgumentName().toString())) {
      programIdParts = arguments.get(type.getArgumentName().toString()).split("\\.");
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
    return String.format("set %s preferences <%s> [<%s>]", type.getName(), ArgumentName.RUNTIME_ARGS,
                         type.getArgumentName());
  }

  @Override
  public String getDescription() {
    return "Sets the preferences of a " + type.getPluralPrettyName() + "." +
      " <" + ArgumentName.RUNTIME_ARGS + "> is specified in the format \"key1=a key2=b\"";
  }
}
