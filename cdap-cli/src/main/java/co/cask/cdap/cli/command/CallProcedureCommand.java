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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.ProcedureClient;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Map;

/**
 * Calls a procedure.
 */
@Deprecated
public class CallProcedureCommand extends AbstractAuthCommand {

  private final ProcedureClient procedureClient;

  @Inject
  public CallProcedureCommand(ProcedureClient procedureClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.procedureClient = procedureClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] appIdAndProcedureId = arguments.get(ArgumentName.PROCEDURE.toString()).split("\\.");
    if (appIdAndProcedureId.length < 2) {
      throw new CommandInputError(this);
    }

    String appId = appIdAndProcedureId[0];
    String procedureId = appIdAndProcedureId[1];
    String methodId = arguments.get(ArgumentName.METHOD.toString());

    Map<String, String> parametersMap = Maps.newHashMap();
    String[] parameters = arguments.get(ArgumentName.PARAMETER_MAP.toString(), "").split(" ");
    for (int i = 0; i < parameters.length; i += 2) {
      String key = parameters[i];
      String value = parameters[i + 1];
      parametersMap.put(key, value);
    }

    String result = procedureClient.call(appId, procedureId, methodId, parametersMap);
    output.println(result);
  }

  @Override
  public String getPattern() {
    return String.format("call procedure <%s> <%s> [<%s>]", ArgumentName.PROCEDURE,
                         ArgumentName.METHOD, ArgumentName.PARAMETER_MAP);
  }

  @Override
  public String getDescription() {
    return "Calls a " + ElementType.PROCEDURE.getPrettyName();
  }
}
