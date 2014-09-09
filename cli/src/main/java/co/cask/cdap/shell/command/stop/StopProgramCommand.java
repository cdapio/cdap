/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command.stop;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.ArgumentName;
import co.cask.cdap.shell.command.Arguments;
import co.cask.cdap.shell.command.Command;

import java.io.PrintStream;

/**
 * Stops a program.
 */
public class StopProgramCommand extends AbstractCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  public StopProgramCommand(ElementType elementType, ProgramClient programClient) {
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(ArgumentName.PROGRAM).split("\\.");
    String appId = programIdParts[0];
    String programId = programIdParts[1];

    programClient.stop(appId, elementType.getProgramType(), programId);
    output.printf("Successfully stopped %s '%s' of application '%s'\n", elementType.getPrettyName(), programId, appId);
  }

  @Override
  public String getPattern() {
    return String.format("stop %s <%s>", elementType.getName(), ArgumentName.PROGRAM);
  }

  @Override
  public String getDescription() {
    return "Stops a " + elementType.getPrettyName();
  }
}
