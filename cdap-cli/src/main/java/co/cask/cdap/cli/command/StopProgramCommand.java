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
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.ProgramClient;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Stops a program.
 */
public class StopProgramCommand extends AbstractAuthCommand implements Categorized {

  private final ProgramClient programClient;
  private final ElementType elementType;

  public StopProgramCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    if (programIdParts.length < 2) {
      throw new CommandInputError(this);
    }

    String appId = programIdParts[0];
    String programId = programIdParts[1];

    programClient.stop(appId, elementType.getProgramType(), programId);
    output.printf("Successfully stopped %s '%s' of application '%s'\n", elementType.getPrettyName(), programId, appId);
  }

  @Override
  public String getPattern() {
    return String.format("stop %s <%s>", elementType.getName(), elementType.getArgumentName());
  }

  @Override
  public String getDescription() {
    return "Stops a " + elementType.getPrettyName();
  }

  @Override
  public String getCategory() {
    return elementType.getCommandCategory().getName();
  }
}
