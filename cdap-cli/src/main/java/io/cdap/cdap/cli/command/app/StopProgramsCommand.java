/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.cli.command.app;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.BatchProgramResult;
import io.cdap.cdap.proto.ProgramRecord;

import java.io.PrintStream;
import java.util.List;

/**
 * Stops one or more programs in an application.
 */
public class StopProgramsCommand extends BaseBatchCommand<BatchProgram> {
  private final ProgramClient programClient;

  @Inject
  public StopProgramsCommand(ApplicationClient appClient, ProgramClient programClient, CLIConfig cliConfig) {
    super(appClient, cliConfig);
    this.programClient = programClient;
  }

  @Override
  protected BatchProgram createProgram(ProgramRecord programRecord) {
    return new BatchProgram(programRecord.getApp(), programRecord.getType(), programRecord.getName());
  }

  @Override
  protected void runBatchCommand(PrintStream printStream, Args<BatchProgram> args) throws Exception {
    List<BatchProgramResult> results = programClient.stop(args.appId.getParent(), args.programs);

    Table table = Table.builder()
      .setHeader("name", "type", "error")
      .setRows(results, new RowMaker<BatchProgramResult>() {
        @Override
        public List<?> makeRow(BatchProgramResult result) {
          return Lists.newArrayList(
            result.getProgramId(), result.getProgramType(), result.getError());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, printStream, table);
  }

  @Override
  public String getPattern() {
    return String.format("stop app <%s> programs [of type <%s>]", ArgumentName.APP, ArgumentName.PROGRAM_TYPES);
  }

  @Override
  public String getDescription() {
    return getDescription("stop", "stops");
  }
}
