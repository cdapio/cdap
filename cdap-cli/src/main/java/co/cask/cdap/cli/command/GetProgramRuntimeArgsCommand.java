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
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.common.cli.Arguments;
import com.google.gson.Gson;

import java.io.PrintStream;
import java.util.Map;

/**
 * Gets the runtime arguments of a program.
 */
public class GetProgramRuntimeArgsCommand extends AbstractAuthCommand {

  private static final Gson GSON = new Gson();

  private final ProgramClient programClient;
  private final ElementType elementType;

  public GetProgramRuntimeArgsCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    ProgramId programId = parseProgramId(arguments, elementType);
    Map<String, String> runtimeArgs = programClient.getRuntimeArgs(programId.toId());
    output.printf(GSON.toJson(runtimeArgs));
  }

  @Override
  public String getPattern() {
    return String.format("get %s runtimeargs <%s> [version <%s>]", elementType.getShortName(),
                         elementType.getArgumentName(), ArgumentName.APP_VERSION);
  }

  @Override
  public String getDescription() {
    return String.format("Gets the runtime arguments of %s", Fragment.of(Article.A, elementType.getName()));
  }
}
