/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.client.ProgramClient;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Gets the instances of a program.
 */
public class GetProgramInstancesCommand extends AbstractAuthCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  protected GetProgramInstancesCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  @SuppressWarnings("deprecation")
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    String appId = programIdParts[0];

    int instances;
    switch (elementType) {
      case FLOWLET:
        if (programIdParts.length < 3) {
          throw new CommandInputError(this);
        }
        String flowId = programIdParts[1];
        String flowletId = programIdParts[2];
        instances = programClient.getFlowletInstances(appId, flowId, flowletId);
        break;
      case WORKER:
        if (programIdParts.length < 2)  {
          throw new CommandInputError(this);
        }
        String workerId = programIdParts[1];
        instances = programClient.getWorkerInstances(appId, workerId);
        break;
      case PROCEDURE:
        if (programIdParts.length < 2) {
          throw new CommandInputError(this);
        }
        String procedureId = programIdParts[1];
        instances = programClient.getProcedureInstances(appId, procedureId);
        break;
      case SERVICE:
        if (programIdParts.length < 2) {
          throw new CommandInputError(this);
        }
        String service = programIdParts[1];
        instances = programClient.getServiceInstances(appId, service);
        break;
      case RUNNABLE:
        if (programIdParts.length < 3) {
          throw new CommandInputError(this);
        }
        String serviceId = programIdParts[1];
        String runnableId = programIdParts[2];
        instances = programClient.getServiceRunnableInstances(appId, serviceId, runnableId);
        break;
      default:
        // TODO: remove this
        throw new IllegalArgumentException("Unrecognized program element type for scaling: " + elementType);
    }

    output.println(instances);
  }

  @Override
  public String getPattern() {
    return String.format("get %s instances <%s>", elementType.getName(), elementType.getArgumentName());
  }

  @Override
  public String getDescription() {
    return String.format("Gets the instances of a %s.", elementType.getPrettyName());
  }
}
