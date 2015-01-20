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
import com.google.common.base.Joiner;

import java.io.PrintStream;
import java.util.Map;

/**
 * Abstract Class for getting preferences for instance, namespace, application, program.
 */
public abstract class AbstractGetPreferencesCommand extends AbstractAuthCommand {
  private final PreferencesClient client;
  private final ElementType type;
  private final boolean resolved;

  protected AbstractGetPreferencesCommand(ElementType type, PreferencesClient client, CLIConfig cliConfig,
                                          boolean resolved) {
    super(cliConfig);
    this.type = type;
    this.client = client;
    this.resolved = resolved;
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

    switch(type) {
      case INSTANCE:
        if (programIdParts.length != 0) {
          throw new CommandInputError(this);
        }
        printStream.print(joinMapEntries(client.getInstancePreferences()));
        break;

      case NAMESPACE:
        if (programIdParts.length != 1) {
          throw new CommandInputError(this);
        }
        printStream.print(joinMapEntries(client.getNamespacePreferences(programIdParts[0], resolved)));
        break;

      case APP:
        // todo : change this to 2 when namespace id is part of the argument.
        if (programIdParts.length != 1) {
          throw new CommandInputError(this);
        }
        printStream.print(joinMapEntries(client.getApplicationPreferences(Constants.DEFAULT_NAMESPACE,
                                                                          programIdParts[0], resolved)));
        break;

      case FLOW:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        printStream.print(joinMapEntries(client.getProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0],
                                                                      type.getPluralName(), programIdParts[1],
                                                                      resolved)));
        break;

      case PROCEDURE:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        printStream.print(joinMapEntries(client.getProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0],
                                                                      type.getPluralName(), programIdParts[1],
                                                                      resolved)));
        break;

      case MAPREDUCE:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        printStream.print(joinMapEntries(client.getProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0],
                                                                      type.getPluralName(), programIdParts[1],
                                                                      resolved)));
        break;

      case WORKFLOW:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        printStream.print(joinMapEntries(client.getProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0],
                                                                      type.getPluralName(), programIdParts[1],
                                                                      resolved)));
        break;

      case SERVICE:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        printStream.print(joinMapEntries(client.getProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0],
                                                                      type.getPluralName(), programIdParts[1],
                                                                      resolved)));
        break;

      case SPARK:
        // todo : change this to 3 when namespace id is part of the argument.
        if (programIdParts.length != 2) {
          throw new CommandInputError(this);
        }
        printStream.print(joinMapEntries(client.getProgramPreferences(Constants.DEFAULT_NAMESPACE, programIdParts[0],
                                                                      type.getPluralName(), programIdParts[1],
                                                                      resolved)));
        break;

      default:
        throw new IllegalArgumentException("Unrecognized Element Type for Preferences "  + type.getPrettyName());
    }
  }
}
