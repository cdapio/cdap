/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.util;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.proto.security.Action;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.io.PrintStream;
import java.util.Set;

/**
 * Abstract command for updating {@link co.cask.cdap.security.authentication.client.AccessToken}.
 */
public abstract class AbstractAuthCommand implements Command {

  protected final CLIConfig cliConfig;

  protected static final Function<String, Set<Action>> ACTIONS_STRING_TO_SET = new Function<String, Set<Action>>() {
    @Override
    public Set<Action> apply(String input) {
      ImmutableSet.Builder<Action> resultBuilder = ImmutableSet.builder();
      for (String action : Splitter.on(",").trimResults().split(input)) {
        resultBuilder.add(Action.valueOf(action.toUpperCase()));
      }
      return resultBuilder.build();
    }
  };

  public AbstractAuthCommand(CLIConfig cliConfig) {
    this.cliConfig = cliConfig;
  }

  @Override
  public void execute(Arguments arguments, PrintStream printStream) throws Exception {
    try {
      perform(arguments, printStream);
    } catch (UnauthenticatedException e) {
      cliConfig.updateAccessToken(printStream);
      perform(arguments, printStream);
    }
  }

  public abstract void perform(Arguments arguments, PrintStream printStream) throws Exception;

  protected ProgramId parseProgramId(Arguments arguments, ElementType elementType) {
    String[] argumentParts = arguments.get(elementType.getArgumentName().getName()).split("\\.");
    if (argumentParts.length < 2) {
      throw new CommandInputError(this);
    }

    String appName = argumentParts[0];
    String appVersion = arguments.hasArgument(ArgumentName.APP_VERSION.getName())
      ? arguments.get(ArgumentName.APP_VERSION.getName()) : ApplicationId.DEFAULT_VERSION;
    String programName = argumentParts[1];
    return cliConfig.getCurrentNamespace().app(appName, appVersion).program(elementType.getProgramType(), programName);
  }

  protected ApplicationId parseApplicationId(Arguments arguments) {
    String appVersion = arguments.hasArgument(ArgumentName.APP_VERSION.toString()) ?
      arguments.get(ArgumentName.APP_VERSION.toString()) : ApplicationId.DEFAULT_VERSION;
    return cliConfig.getCurrentNamespace().app(arguments.get(ArgumentName.APP.toString()), appVersion);
  }
}
