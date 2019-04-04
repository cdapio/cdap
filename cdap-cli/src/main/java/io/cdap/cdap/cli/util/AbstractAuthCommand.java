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

package io.cdap.cdap.cli.util;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.exception.CommandInputError;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.common.cli.Arguments;
import io.cdap.common.cli.Command;

import java.io.PrintStream;
import java.util.Set;

/**
 * Abstract command for updating {@link io.cdap.cdap.security.authentication.client.AccessToken}.
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
