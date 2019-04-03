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

import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.ArgumentParser;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.Map;

/**
 * Sets the runtime arguments of a program.
 */
public class SetProgramRuntimeArgsCommand extends AbstractAuthCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  public SetProgramRuntimeArgsCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    ProgramId programId = parseProgramId(arguments, elementType);
    String appName = programId.getApplication();
    String appVersion = programId.getVersion();
    String programName = programId.getProgram();
    String runtimeArgsString = arguments.get(ArgumentName.RUNTIME_ARGS.toString());
    Map<String, String> runtimeArgs = ArgumentParser.parseMap(runtimeArgsString, ArgumentName.RUNTIME_ARGS.toString());
    programClient.setRuntimeArgs(programId, runtimeArgs);
    output.printf("Successfully set runtime args of %s '%s' of application '%s.%s' to '%s'\n",
                  elementType.getName(), programName, appName, appVersion, runtimeArgsString);
  }

  @Override
  public String getPattern() {
    return String.format("set %s runtimeargs <%s> [version <%s>] <%s>", elementType.getShortName(),
                         elementType.getArgumentName(), ArgumentName.APP_VERSION, ArgumentName.RUNTIME_ARGS);
  }

  @Override
  public String getDescription() {
    return String.format("Sets the runtime arguments of %s. '<%s>' is specified in the format 'key1=v1 key2=v2'.",
                         Fragment.of(Article.A, elementType.getName()), ArgumentName.RUNTIME_ARGS);
  }
}
