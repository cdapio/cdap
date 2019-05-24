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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.cli.Arguments;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for commands that work on multiple programs in an application.
 *
 * @param <T> the type of input object for the batch request
 */
public abstract class BaseBatchCommand<T extends BatchProgram> extends AbstractAuthCommand {
  private final ApplicationClient appClient;

  protected BaseBatchCommand(ApplicationClient appClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.appClient = appClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    Args<T> args = readArgs(arguments);
    if (args.programs.isEmpty()) {
      output.printf(String.format("application '%s' contains no programs of type '%s'",
        args.appId.getEntityName(), Joiner.on(',').join(args.programTypes)));
      return;
    }

    runBatchCommand(output, args);
  }

  /**
   * Reads arguments to get app id, program types, and list of input programs.
   */
  protected Args<T> readArgs(Arguments arguments)
    throws ApplicationNotFoundException, UnauthenticatedException, IOException, UnauthorizedException {

    String appName = arguments.get(ArgumentName.APP.getName());
    ApplicationId appId = cliConfig.getCurrentNamespace().app(appName);

    Set<ProgramType> programTypes = getDefaultProgramTypes();
    if (arguments.hasArgument(ArgumentName.PROGRAM_TYPES.getName())) {
      programTypes.clear();
      String programTypesStr = arguments.get(ArgumentName.PROGRAM_TYPES.getName());
      for (String programTypeStr : Splitter.on(',').trimResults().split(programTypesStr)) {
        ProgramType programType = ProgramType.valueOf(programTypeStr.toUpperCase());
        programTypes.add(programType);
      }
    }

    List<T> programs = new ArrayList<>();
    Map<ProgramType, List<ProgramRecord>> appPrograms = appClient.listProgramsByType(appId);
    for (ProgramType programType : programTypes) {
      List<ProgramRecord> programRecords = appPrograms.get(programType);
      if (programRecords != null) {
        for (ProgramRecord programRecord : programRecords) {
          programs.add(createProgram(programRecord));
        }
      }
    }
    return new Args<>(appId, programTypes, programs);
  }

  protected Set<ProgramType> getDefaultProgramTypes() {
    Set<ProgramType> types = new HashSet<>();
    types.add(ProgramType.SERVICE);
    types.add(ProgramType.WORKER);
    return types;
  }

  protected String getDescription(String action, String actionPlural) {
    return String.format("Command to %s one or more programs of %s. " +
        "By default, %s all services and workers. A comma-separated list of program types can be " +
        "specified, which will %s all programs of those types. For example, specifying 'service,workflow' will %s " +
        "all services and workflows in the %s.",
      action, Fragment.of(Article.A, ElementType.APP.getName()), actionPlural, action, action, 
      ElementType.APP.getName());
  }

  /**
   * Create an input object from a ProgramRecord.
   */
  protected abstract T createProgram(ProgramRecord programRecord);

  /**
   * Run the batch command on all programs.
   */
  protected abstract void runBatchCommand(PrintStream printStream, Args<T> args) throws Exception;

  /**
   * Container for command arguments.
   * @param <T> type of input object
   */
  protected static class Args<T> {
    protected final ApplicationId appId;
    protected final Set<ProgramType> programTypes;
    protected final List<T> programs;

    public Args(ApplicationId appId, Set<ProgramType> programTypes, List<T> programs) {
      this.appId = appId;
      this.programTypes = programTypes;
      this.programs = programs;
    }
  }
}
