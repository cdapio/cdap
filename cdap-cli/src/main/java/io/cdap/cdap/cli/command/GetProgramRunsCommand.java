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

import com.google.common.collect.Lists;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.exception.CommandInputError;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.List;

/**
 * Gets the run records of a program.
 */
public class GetProgramRunsCommand extends AbstractCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  protected GetProgramRunsCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    String appId = programIdParts[0];
    long currentTime = System.currentTimeMillis();

    long startTime = getTimestamp(arguments.getOptional(ArgumentName.START_TIME.toString(), "min"), currentTime);
    long endTime = getTimestamp(arguments.getOptional(ArgumentName.END_TIME.toString(), "max"), currentTime);
    int limit = arguments.getIntOptional(ArgumentName.LIMIT.toString(), Integer.MAX_VALUE);

    List<RunRecord> records;
    if (elementType.getProgramType() != null) {
      if (programIdParts.length < 2) {
        throw new CommandInputError(this);
      }
      String programName = programIdParts[1];
      ProgramId programId = cliConfig.getCurrentNamespace().app(appId).program(elementType.getProgramType(),
                                                                               programName);
      if (arguments.hasArgument(ArgumentName.RUN_STATUS.toString())) {
        records = programClient.getProgramRuns(programId, arguments.get(ArgumentName.RUN_STATUS.toString()),
                                               startTime, endTime, limit);
      } else {
        records = programClient.getAllProgramRuns(programId, startTime, endTime, limit);
      }

    } else {
      throw new IllegalArgumentException("Unrecognized program element type for history: " + elementType);
    }

    Table table = Table.builder()
      .setHeader("pid", "end status", "init time", "start time", "stop time")
      .setRows(records, new RowMaker<RunRecord>() {
        @Override
        public List<?> makeRow(RunRecord object) {
          return Lists.newArrayList(object.getPid(), object.getStatus(), object.getStartTs(),
                                    object.getRunTs() == null ? "" : object.getRunTs(),
                                    object.getStopTs() == null ? "" : object.getStopTs());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get %s runs <%s> [<%s>] [<%s>] [<%s>] [<%s>]", elementType.getShortName(),
                         elementType.getArgumentName(), ArgumentName.RUN_STATUS,
                         ArgumentName.START_TIME, ArgumentName.END_TIME, ArgumentName.LIMIT);
  }

  @Override
  public String getDescription() {
    return String.format("Gets the run history of %s", Fragment.of(Article.A, elementType.getName()));
  }
}
