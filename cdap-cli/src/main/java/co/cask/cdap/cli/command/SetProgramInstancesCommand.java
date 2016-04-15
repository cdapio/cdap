/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.Id;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Sets the instances of a program.
 */
public class SetProgramInstancesCommand extends AbstractAuthCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  public SetProgramInstancesCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    Id.Application appId = Id.Application.from(cliConfig.getCurrentNamespace(), programIdParts[0]);
    int numInstances = arguments.getInt(ArgumentName.NUM_INSTANCES.toString());

    switch (elementType) {
      case FLOWLET:
        if (programIdParts.length < 3) {
          throw new CommandInputError(this);
        }
        String flowId = programIdParts[1];
        String flowletName = programIdParts[2];
        Id.Flow.Flowlet flowletId = Id.Flow.Flowlet.from(appId, flowId, flowletName);
        programClient.setFlowletInstances(flowletId, numInstances);
        output.printf("Successfully set flowlet '%s' of flow '%s' of app '%s' to %d instances\n",
                      flowId, flowletId, appId.getId(), numInstances);
        break;
      case WORKER:
        if (programIdParts.length < 2) {
          throw new CommandInputError(this);
        }
        String workerName = programIdParts[1];
        Id.Worker workerId = Id.Worker.from(appId, workerName);
        programClient.setWorkerInstances(workerId, numInstances);
        output.printf("Successfully set worker '%s' of app '%s' to %d instances\n",
                      workerName, appId.getId(), numInstances);
        break;
      case SERVICE:
        if (programIdParts.length < 2) {
          throw new CommandInputError(this);
        }
        String serviceName = programIdParts[1];
        Id.Service service = Id.Service.from(appId, serviceName);
        programClient.setServiceInstances(service, numInstances);
        output.printf("Successfully set service '%s' of app '%s' to %d instances\n",
                      serviceName, appId.getId(), numInstances);
        break;
      default:
        // TODO: remove this
        throw new IllegalArgumentException("Unrecognized program element type for scaling: " + elementType);
    }
  }

  @Override
  public String getPattern() {
    return String.format("set %s instances <%s> <%s>", elementType.getName(),
                         elementType.getArgumentName(), ArgumentName.NUM_INSTANCES);
  }

  @Override
  public String getDescription() {
    return String.format("Sets the number of instances of %s", Fragment.of(Article.A, elementType.getName()));

  }
}
