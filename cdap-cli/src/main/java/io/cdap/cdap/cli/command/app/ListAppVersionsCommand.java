/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.common.cli.Arguments;
import java.io.PrintStream;
import java.util.List;

/**
 * Lists all versions of a specific app.
 */
public class ListAppVersionsCommand extends AbstractAuthCommand {

  private final ApplicationClient appClient;

  @Inject
  public ListAppVersionsCommand(ApplicationClient appClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.appClient = appClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String appName = arguments.get(ArgumentName.APP.toString());
    List<String> versions = appClient.listAppVersions(cliConfig.getCurrentNamespace(), appName);

    Table table = Table.builder()
        .setHeader("version")
        .setRows(versions, new RowMaker<String>() {
          @Override
          public List<String> makeRow(String version) {
            return Lists.newArrayList(version);
          }
        }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("list app versions <%s>", ArgumentName.APP);
  }

  @Override
  public String getDescription() {
    return String.format("Lists all versions of a specific %s", ElementType.APP.getName());
  }
}
