/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.cli.command;

import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.exception.CommandInputError;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.common.cli.Arguments;

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
    ApplicationId appId = cliConfig.getCurrentNamespace().app(programIdParts[0]);

    int instances;
    switch (elementType) {
      case WORKER:
        if (programIdParts.length < 2)  {
          throw new CommandInputError(this);
        }
        String workerId = programIdParts[1];
        ProgramId worker = appId.worker(workerId);
        instances = programClient.getWorkerInstances(worker);
        break;
      case SERVICE:
        if (programIdParts.length < 2) {
          throw new CommandInputError(this);
        }
        String serviceName = programIdParts[1];
        instances = programClient.getServiceInstances(appId.service(serviceName));
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
    return String.format("Gets the number of instances of %s", Fragment.of(Article.A, elementType.getName()));
  }
}
