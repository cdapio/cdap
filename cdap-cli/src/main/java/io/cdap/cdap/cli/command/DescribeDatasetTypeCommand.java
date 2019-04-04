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
import io.cdap.cdap.client.DatasetTypeClient;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.List;

/**
 * Shows information about a dataset type.
 */
public class DescribeDatasetTypeCommand extends AbstractAuthCommand {

  private final DatasetTypeClient datasetTypeClient;

  @Inject
  public DescribeDatasetTypeCommand(DatasetTypeClient datasetTypeClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.datasetTypeClient = datasetTypeClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    DatasetTypeId type = cliConfig.getCurrentNamespace().datasetType(
      arguments.get(ArgumentName.DATASET_TYPE.toString()));
    DatasetTypeMeta datasetTypeMeta = datasetTypeClient.get(type);

    Table table = Table.builder()
      .setHeader("name", "modules")
      .setRows(ImmutableList.of(datasetTypeMeta), new RowMaker<DatasetTypeMeta>() {
        @Override
        public List<?> makeRow(DatasetTypeMeta object) {
          return Lists.newArrayList(object.getName(), Joiner.on(", ").join(object.getModules()));
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("describe dataset type <%s>", ArgumentName.DATASET_TYPE);
  }

  @Override
  public String getDescription() {
    return String.format("Describes %s",
                         Fragment.of(Article.A, ElementType.DATASET_TYPE.getName()));
  }
}
