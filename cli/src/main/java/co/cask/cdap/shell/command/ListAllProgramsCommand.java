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
import com.google.common.collect.Lists;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Lists all programs.
 */
public class ListAllProgramsCommand extends AbstractCommand {

  private final ApplicationClient appClient;

  @Inject
  public ListAllProgramsCommand(ApplicationClient appClient) {
    super("programs", null, "Lists all " + ElementType.PROGRAM.getPluralPrettyName());
    this.appClient = appClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    Map<ProgramType, List<ProgramRecord>> allPrograms = appClient.listAllPrograms();
    List<ProgramRecord> allProgramsList = Lists.newArrayList();
    for (List<ProgramRecord> subList : allPrograms.values()) {
      allProgramsList.addAll(subList);
    }

    new AsciiTable<ProgramRecord>(
      new String[] { "type", "app", "id", "description" },
      allProgramsList,
      new RowMaker<ProgramRecord>() {
        @Override
        public Object[] makeRow(ProgramRecord object) {
          return new Object[] { object.getType().getCategoryName(), object.getApp(),
            object.getId(), object.getDescription() };
        }
      }
    ).print(output);
  }
}
