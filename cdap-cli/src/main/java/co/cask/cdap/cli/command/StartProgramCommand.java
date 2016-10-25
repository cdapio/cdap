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
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.ArgumentParser;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.common.cli.Arguments;
import com.google.gson.Gson;

import java.io.PrintStream;
import java.util.Map;

/**
 * Starts a program.
 */
public class StartProgramCommand extends AbstractAuthCommand {
  private static final Gson GSON = new Gson();

  protected final ElementType elementType;
  private final ProgramClient programClient;

  protected boolean isDebug = false;

  public StartProgramCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    if (programIdParts.length < 2) {
      throw new CommandInputError(this);
    }

    ProgramId programId = parseProgramId(arguments, elementType);
    String appName = programId.getApplication();
    String appVersion = programId.getVersion();
    String programName = programId.getProgram();

    String runtimeArgsString = arguments.getOptional(ArgumentName.RUNTIME_ARGS.toString(), "");
    if (runtimeArgsString == null || runtimeArgsString.isEmpty()) {
      // run with stored runtime args
      programClient.start(programId, isDebug, null);
      runtimeArgsString = GSON.toJson(programClient.getRuntimeArgs(programId));
      output.printf("Successfully started %s '%s' of application '%s.%s' with stored runtime arguments '%s'\n",
                    elementType.getName(), programName, appName, appVersion, runtimeArgsString);
    } else {
      // run with user-provided runtime args
      Map<String, String> runtimeArgs = ArgumentParser.parseMap(runtimeArgsString);
      programClient.start(programId, isDebug, runtimeArgs);
      output.printf("Successfully started %s '%s' of application '%s.%s' with provided runtime arguments '%s'\n",
                    elementType.getName(), programName, appName, appVersion, runtimeArgsString);
    }

  }

  @Override
  public String getPattern() {
    return String.format("start %s <%s> [version <%s>] [<%s>]", elementType.getShortName(),
                         elementType.getArgumentName(), ArgumentName.APP_VERSION, ArgumentName.RUNTIME_ARGS);
  }

  @Override
  public String getDescription() {
    return String.format("Starts %s. '<%s>' is specified in the format 'key1=a key2=b'.",
      Fragment.of(Article.A, elementType.getName()), ArgumentName.RUNTIME_ARGS);
  }
}
