/*
 * Copyright © 2012-2017 Cask Data, Inc.
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

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.List;

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
    ApplicationId appId = parseApplicationId(arguments);
    List<ProgramRecord> programsList = applicationClient.listPrograms(appId);

    Table table = Table.builder()
      .setHeader("type", "id", "description")
      .setRows(programsList, new RowMaker<ProgramRecord>() {
        @Override
        public List<?> makeRow(ProgramRecord object) {
          return Lists.newArrayList(object.getType().getPrettyName(), object.getName(), object.getDescription());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("describe app <%s> [version <%s>]", ArgumentName.APP, ArgumentName.APP_VERSION);
  }

  @Override
  public String getDescription() {
    return String.format("Describes %s with an optional version. If version is not provided, default version '%s' " +
                           "will be used.", Fragment.of(Article.A, ElementType.APP.getName()),
                         ApplicationId.DEFAULT_VERSION);
  }
}
