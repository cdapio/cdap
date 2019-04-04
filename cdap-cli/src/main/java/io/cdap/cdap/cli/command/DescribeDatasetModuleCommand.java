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

package io.cdap.cdap.cli.command;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
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
import io.cdap.cdap.client.DatasetModuleClient;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.List;

/**
 * Shows information about a dataset module.
 */
public class DescribeDatasetModuleCommand extends AbstractAuthCommand {

  private final DatasetModuleClient datasetModuleClient;

  @Inject
  public DescribeDatasetModuleCommand(DatasetModuleClient datasetModuleClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.datasetModuleClient = datasetModuleClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    DatasetModuleId moduleId = cliConfig.getCurrentNamespace().datasetModule(
      arguments.get(ArgumentName.DATASET_MODULE.toString()));
    DatasetModuleMeta datasetModuleMeta = datasetModuleClient.get(moduleId);

    Table table = Table.builder()
      .setHeader("name", "className", "jarLocationPath", "types", "usesModules", "usedByModules")
      .setRows(ImmutableList.of(datasetModuleMeta), new RowMaker<DatasetModuleMeta>() {
        @Override
        public List<?> makeRow(DatasetModuleMeta object) {
          return Lists.newArrayList(object.getName(), object.getClassName(), object.getJarLocationPath(),
                                    Joiner.on(", ").join(object.getTypes()),
                                    Joiner.on(", ").join(object.getUsesModules()),
                                    Joiner.on(", ").join(object.getUsedByModules()));
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("describe dataset module <%s>", ArgumentName.DATASET_MODULE);
  }

  @Override
  public String getDescription() {
    return String.format("Describes %s",
                         Fragment.of(Article.A, ElementType.DATASET_MODULE.getName()));
  }
}
