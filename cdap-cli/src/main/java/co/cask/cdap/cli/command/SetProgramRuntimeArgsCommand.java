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
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.ArgumentParser;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.common.cli.Arguments;
import com.google.gson.Gson;

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
    Map<String, String> runtimeArgs = ArgumentParser.parseMap(runtimeArgsString);
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
