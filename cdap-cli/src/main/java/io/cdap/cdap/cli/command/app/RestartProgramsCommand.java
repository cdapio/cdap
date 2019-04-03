/*
 * Copyright © 2015 Cask Data, Inc.
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

import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.BatchProgramStart;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Restarts one or more programs in an application.
 */
public class RestartProgramsCommand extends BaseBatchCommand<BatchProgram> {
  private final ProgramClient programClient;

  @Inject
  public RestartProgramsCommand(ApplicationClient appClient, ProgramClient programClient, CLIConfig cliConfig) {
    super(appClient, cliConfig);
    this.programClient = programClient;
  }

  @Override
  protected BatchProgram createProgram(ProgramRecord programRecord) {
    return new BatchProgram(programRecord.getApp(), programRecord.getType(), programRecord.getName());
  }

  @Override
  protected void runBatchCommand(PrintStream printStream, Args<BatchProgram> args) throws Exception {
    NamespaceId namespace = args.appId.getParent();

    printStream.print("Stopping programs...\n");
    programClient.stop(namespace, args.programs);

    printStream.print("Starting programs...\n");
    List<BatchProgramStart> startList = new ArrayList<>(args.programs.size());
    for (BatchProgram program : args.programs) {
      startList.add(new BatchProgramStart(program));
    }
    programClient.start(namespace, startList);
  }

  @Override
  public String getPattern() {
    return String.format("restart app <%s> programs [of type <%s>]", ArgumentName.APP, ArgumentName.PROGRAM_TYPES);
  }

  @Override
  public String getDescription() {
    return getDescription("restart", "restarts");
  }
}
