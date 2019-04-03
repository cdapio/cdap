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

package io.cdap.cdap.cli.command;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.DatasetModuleClient;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists all dataset modules.
 */
public class ListDatasetModulesCommand extends AbstractAuthCommand {

  private final DatasetModuleClient client;

  @Inject
  public ListDatasetModulesCommand(DatasetModuleClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    List<DatasetModuleMeta> modules = client.list(cliConfig.getCurrentNamespace());
    Table table = Table.builder()
      .setHeader("name", "className")
      .setRows(modules, new RowMaker<DatasetModuleMeta>() {
        @Override
        public List<?> makeRow(DatasetModuleMeta object) {
          return Lists.newArrayList(object.getName(), object.getClassName());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return "list dataset modules";
  }

  @Override
  public String getDescription() {
    return String.format("Lists all %s", ElementType.DATASET_MODULE.getNamePlural());
  }
}
