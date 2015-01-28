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

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.AsciiTable;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists all programs of a certain type.
 */
public class ListProgramsCommand extends AbstractAuthCommand implements Categorized {

  private final ApplicationClient appClient;
  private final ProgramType programType;

  public ListProgramsCommand(ProgramType programType, ApplicationClient appClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.programType = programType;
    this.appClient = appClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    List<ProgramRecord> programs = appClient.listAllPrograms(programType);
    new AsciiTable<ProgramRecord>(
      new String[] { "app", "id", "description" },
      programs,
      new RowMaker<ProgramRecord>() {
        @Override
        public Object[] makeRow(ProgramRecord object) {
          return new Object[] { object.getApp(), object.getId(), object.getDescription() };
        }
      }).print(output);
  }

  @Override
  public String getPattern() {
    return String.format("list %s", programType.getCategoryName());
  }

  @Override
  public String getDescription() {
    return "Lists " + getElementType().getPluralPrettyName();
  }

  @Override
  public String getCategory() {
    return getElementType().getCommandCategory().getName();
  }

  private ElementType getElementType() {
    return ElementType.fromProgramType(programType);
  }
}
