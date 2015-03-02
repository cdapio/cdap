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
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.common.cli.Arguments;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists all applications.
 */
public class ListAppsCommand extends AbstractAuthCommand {

  private final ApplicationClient appClient;
  private final TableRenderer tableRenderer;

  @Inject
  public ListAppsCommand(ApplicationClient appClient, CLIConfig cliConfig, TableRenderer tableRenderer) {
    super(cliConfig);
    this.appClient = appClient;
    this.tableRenderer = tableRenderer;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    Table table = Table.builder()
      .setHeader("id", "description")
      .setRows(appClient.list(), new RowMaker<ApplicationRecord>() {
        @Override
        public List<?> makeRow(ApplicationRecord object) {
          return ImmutableList.of(object.getId(), object.getDescription());
        }
      }).build();
    tableRenderer.render(output, table);
  }

  @Override
  public String getPattern() {
    return "list apps";
  }

  @Override
  public String getDescription() {
    return String.format("Lists all %s.", ElementType.APP.getPluralPrettyName());
  }
}
