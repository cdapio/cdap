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
import co.cask.cdap.client.DatasetModuleClient;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.common.cli.Arguments;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists all dataset modules.
 */
public class ListDatasetModulesCommand extends AbstractAuthCommand {

  private final DatasetModuleClient client;
  private final TableRenderer tableRenderer;

  @Inject
  public ListDatasetModulesCommand(DatasetModuleClient client, CLIConfig cliConfig, TableRenderer tableRenderer) {
    super(cliConfig);
    this.client = client;
    this.tableRenderer = tableRenderer;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    List<DatasetModuleMeta> modules = client.list();
    Table table = Table.builder()
      .setHeader("name", "className")
      .setRows(modules, new RowMaker<DatasetModuleMeta>() {
        @Override
        public List<?> makeRow(DatasetModuleMeta object) {
          return ImmutableList.of(object.getName(), object.getClassName());
        }
      }).build();
    tableRenderer.render(output, table);
  }

  @Override
  public String getPattern() {
    return "list dataset modules";
  }

  @Override
  public String getDescription() {
    return String.format("Lists all %s.", ElementType.DATASET_MODULE.getPluralPrettyName());
  }
}
