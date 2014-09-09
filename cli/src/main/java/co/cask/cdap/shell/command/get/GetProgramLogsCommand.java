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

package co.cask.cdap.shell.command.get;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.ArgumentName;
import co.cask.cdap.shell.command.Arguments;
import co.cask.cdap.shell.command.Command;

import java.io.PrintStream;

/**
 * Gets the logs of a program.
 */
public class GetProgramLogsCommand extends AbstractCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  protected GetProgramLogsCommand(ElementType elementType, ProgramClient programClient) {
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(ArgumentName.PROGRAM).split("\\.");
    String appId = programIdParts[0];
    long start = arguments.getLong(ArgumentName.START_TIME, 0);
    long stop = arguments.getLong(ArgumentName.START_TIME, Long.MAX_VALUE);

    String logs;
    if (elementType == ElementType.RUNNABLE) {
      String serviceId = programIdParts[1];
      String runnableId = programIdParts[2];
      logs = programClient.getServiceRunnableLogs(appId, serviceId, runnableId, start, stop);
    } else if (elementType.getProgramType() != null) {
      String programId = programIdParts[1];
      logs = programClient.getProgramLogs(appId, elementType.getProgramType(), programId, start, stop);
    } else {
      throw new IllegalArgumentException("Cannot get logs for " + elementType.getPluralName());
    }

    output.println(logs);
  }

  @Override
  public String getPattern() {
    return String.format("get %s logs <%s> [%s] [%s]", elementType.getName(), ArgumentName.PROGRAM,
                         ArgumentName.START_TIME, ArgumentName.END_TIME);
  }

  @Override
  public String getDescription() {
    return "Gets the logs of a " + elementType.getPrettyName();
  }
}
