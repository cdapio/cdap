/*
 * Copyright © 2012-2014 Cask Data, Inc.
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
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.AsciiTable;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Shows detailed information about an application.
 */
public class DescribeAppCommand extends AbstractAuthCommand {

  private final ApplicationClient applicationClient;

  @Inject
  public DescribeAppCommand(ApplicationClient applicationClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.applicationClient = applicationClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String appId = arguments.get(ArgumentName.APP.toString());
    Map<ProgramType, List<ProgramRecord>> programs = applicationClient.listPrograms(appId);
    List<ProgramRecord> programsList = Lists.newArrayList();
    for (List<ProgramRecord> subList : programs.values()) {
      programsList.addAll(subList);
    }

    new AsciiTable<ProgramRecord>(
      new String[] { "app", "type", "id", "description" },
      programsList,
      new RowMaker<ProgramRecord>() {
        @Override
        public Object[] makeRow(ProgramRecord object) {
          return new Object[] { object.getApp(), object.getType().getCategoryName(),
            object.getId(), object.getDescription() };
        }
      }
    ).print(output);
  }

  @Override
  public String getPattern() {
    return String.format("describe app <%s>", ArgumentName.APP);
  }

  @Override
  public String getDescription() {
    return "Shows detailed information about an " + ElementType.APP.getPrettyName();
  }
}
