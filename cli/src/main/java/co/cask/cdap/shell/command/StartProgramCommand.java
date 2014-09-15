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

package co.cask.cdap.shell.command;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ArgumentName;
import co.cask.cdap.shell.Arguments;
import co.cask.cdap.shell.ElementType;

import java.io.PrintStream;

/**
 * Starts a program.
 */
public class StartProgramCommand extends AbstractCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  public StartProgramCommand(ElementType elementType, ProgramClient programClient) {
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(ArgumentName.PROGRAM).split("\\.");
    String appId = programIdParts[0];
    String programId = programIdParts[1];

    programClient.start(appId, elementType.getProgramType(), programId);
    output.printf("Successfully started %s '%s' of application '%s'\n", elementType.getPrettyName(), programId, appId);
  }

  @Override
  public String getPattern() {
    return String.format("start %s <%s>", elementType.getName(), elementType.getArgumentName());
  }

  @Override
  public String getDescription() {
    return "Starts a " + elementType.getPrettyName();
  }
}
