/*
 * Copyright 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command.call;

import co.cask.cdap.client.ProcedureClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.ArgumentName;
import co.cask.cdap.shell.command.Arguments;
import com.google.common.collect.Maps;

import java.io.PrintStream;
import java.util.Map;
import javax.inject.Inject;

/**
 * Calls a procedure.
 */
public class CallProcedureCommand extends AbstractCommand {

  private final ProcedureClient procedureClient;

  @Inject
  public CallProcedureCommand(ProcedureClient procedureClient) {
    this.procedureClient = procedureClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String[] appIdAndProcedureId = arguments.get(ArgumentName.PROCEDURE).split("\\.");
    String appId = appIdAndProcedureId[0];
    String procedureId = appIdAndProcedureId[1];
    String methodId = arguments.get(ArgumentName.METHOD);

    Map<String, String> parametersMap = Maps.newHashMap();
    String prefix = String.format("call procedure %s %s ", arguments.get(ArgumentName.PROCEDURE), methodId);
    if (arguments.getRawInput().length() > prefix.length()) {
      String[] parameters = arguments.getRawInput().substring(prefix.length()).split(" ");
      for (int i = 0; i < parameters.length; i += 2) {
        String key = parameters[i];
        String value = parameters[i + 1];
        parametersMap.put(key, value);
      }
    }

    String result = procedureClient.call(appId, procedureId, methodId, parametersMap);
    output.println(result);
  }

  @Override
  public String getPattern() {
    return String.format("call procedure <%s> <%s> [%s]", ArgumentName.PROCEDURE,
                         ArgumentName.METHOD, ArgumentName.PARAMETER_MAP);
  }

  @Override
  public String getDescription() {
    return "Calls a " + ElementType.PROCEDURE.getPrettyName();
  }
}
