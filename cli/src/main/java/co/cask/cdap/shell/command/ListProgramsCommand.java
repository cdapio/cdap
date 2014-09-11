/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.util.AsciiTable;
import co.cask.cdap.shell.util.RowMaker;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists all programs of a certain type.
 */
public class ListProgramsCommand extends AbstractCommand {

  private final ApplicationClient appClient;
  private final ProgramType programType;

  public ListProgramsCommand(ProgramType programType, ApplicationClient appClient) {
    super(programType.getCategoryName(), null,
          "Lists " + ElementType.fromProgramType(programType).getPluralPrettyName());
    this.programType = programType;
    this.appClient = appClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

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
}
