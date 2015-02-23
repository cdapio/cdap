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

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.RunRecord;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;
import java.util.List;

/**
 * Gets the run records of a program.
 */
public class GetProgramRunsCommand extends AbstractCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;
  private final TableRenderer tableRenderer;

  protected GetProgramRunsCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig,
                                  TableRenderer tableRenderer) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
    this.tableRenderer = tableRenderer;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    String appId = programIdParts[0];
    long currentTime = System.currentTimeMillis();

    long startTime = getTimestamp(arguments.get(ArgumentName.START_TIME.toString(), "min"), currentTime);
    long endTime = getTimestamp(arguments.get(ArgumentName.END_TIME.toString(), "max"), currentTime);
    int limit = arguments.getInt(ArgumentName.LIMIT.toString(), Integer.MAX_VALUE);

    List<RunRecord> records;
    if (elementType.getProgramType() != null) {
      if (programIdParts.length < 2) {
        throw new CommandInputError(this);
      }
      String programId = programIdParts[1];
      if (arguments.hasArgument(ArgumentName.RUN_STATUS.toString())) {
        records = programClient.getProgramRuns(appId, elementType.getProgramType(), programId,
                                               arguments.get(ArgumentName.RUN_STATUS.toString()),
                                               startTime, endTime, limit);
      } else {
        records = programClient.getAllProgramRuns(appId, elementType.getProgramType(), programId,
                                                  startTime, endTime, limit);
      }

    } else {
      throw new IllegalArgumentException("Unrecognized program element type for history: " + elementType);
    }

    Table table = Table.builder()
      .setHeader("pid", "end status", "start", "stop")
      .setRows(records, new RowMaker<RunRecord>() {
        @Override
        public Object[] makeRow(RunRecord object) {
          return new Object[] { object.getPid(), object.getStatus(), object.getStartTs(), object.getStopTs() };
        }
      }).build();
    tableRenderer.render(output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get %s runs <%s> [<%s>] [<%s>] [<%s>] [<%s>]", elementType.getName(),
                         elementType.getArgumentName(), ArgumentName.RUN_STATUS,
                         ArgumentName.START_TIME, ArgumentName.END_TIME, ArgumentName.LIMIT);
  }

  @Override
  public String getDescription() {
    return String.format("Gets the run history of a %s.", elementType.getPrettyName());
  }
}
