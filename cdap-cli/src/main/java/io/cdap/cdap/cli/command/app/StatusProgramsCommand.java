/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import io.cdap.cdap.proto.BatchProgramStart;
import io.cdap.cdap.proto.BatchProgramStatus;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Gets status of one or more programs in an application.
 */
public class StatusProgramsCommand extends BaseBatchCommand<BatchProgram> {
  private final ProgramClient programClient;

  @Inject
  public StatusProgramsCommand(ApplicationClient appClient, ProgramClient programClient, CLIConfig cliConfig) {
    super(appClient, cliConfig);
    this.programClient = programClient;
  }

  @Override
  protected BatchProgramStart createProgram(ProgramRecord programRecord) {
    return new BatchProgramStart(programRecord.getApp(), programRecord.getType(), programRecord.getName());
  }

  @Override
  protected void runBatchCommand(PrintStream printStream, Args<BatchProgram> args) throws Exception {
    List<BatchProgramStatus> results = programClient.getStatus(args.appId.getParent(), args.programs);

    Table table = Table.builder()
      .setHeader("name", "type", "status", "error")
      .setRows(results, new RowMaker<BatchProgramStatus>() {
        @Override
        public List<?> makeRow(BatchProgramStatus result) {
          return Lists.newArrayList(
            result.getProgramId(), result.getProgramType(), result.getStatus(), result.getError());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, printStream, table);
  }

  @Override
  protected Set<ProgramType> getDefaultProgramTypes() {
    Set<ProgramType> types = new HashSet<>();
    types.add(ProgramType.MAPREDUCE);
    types.add(ProgramType.SERVICE);
    types.add(ProgramType.SPARK);
    types.add(ProgramType.WORKER);
    types.add(ProgramType.WORKFLOW);
    return types;
  }

  @Override
  public String getPattern() {
    return String.format("get app <%s> programs status [of type <%s>]", ArgumentName.APP, ArgumentName.PROGRAM_TYPES);
  }

  @Override
  public String getDescription() {
    return getDescription("get status of", "gets status of");
  }
}
