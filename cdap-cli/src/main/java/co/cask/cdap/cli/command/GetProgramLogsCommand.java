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
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Gets the logs of a program.
 */
public class GetProgramLogsCommand extends AbstractAuthCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  protected GetProgramLogsCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    String appId = programIdParts[0];
    String startString = arguments.getOptional(ArgumentName.START_TIME.toString(), "0");
    long start = TimeMathParser.parseTimeInSeconds(startString);
    String stopString = arguments.getOptional(ArgumentName.END_TIME.toString(), Long.toString(Integer.MAX_VALUE));
    long stop = TimeMathParser.parseTimeInSeconds(stopString);

    String logs;
    if (elementType.getProgramType() != null) {
      if (programIdParts.length < 2) {
        throw new CommandInputError(this);
      }
      String programName = programIdParts[1];
      ProgramId programId = cliConfig.getCurrentNamespace().app(appId).program(elementType.getProgramType(),
                                                                               programName);
      logs = programClient.getProgramLogs(programId.toId(), start, stop);
    } else {
      throw new IllegalArgumentException("Cannot get logs for " + elementType.getNamePlural());
    }

    output.println(logs);
  }

  @Override
  public String getPattern() {
    return String.format("get %s logs <%s> [<%s>] [<%s>]", elementType.getShortName(), elementType.getArgumentName(),
                         ArgumentName.START_TIME, ArgumentName.END_TIME);
  }

  @Override
  public String getDescription() {
    return String.format("Gets the logs of %s", Fragment.of(Article.A, elementType.getName()));
  }
}
