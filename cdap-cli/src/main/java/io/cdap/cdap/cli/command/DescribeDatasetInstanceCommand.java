/*
 * Copyright © 2015 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.List;

/**
 * Shows information about a dataset instance.
 */
public class DescribeDatasetInstanceCommand extends AbstractAuthCommand {

  private static final Gson GSON = new Gson();
  private final DatasetClient datasetClient;

  @Inject
  public DescribeDatasetInstanceCommand(DatasetClient datasetClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.datasetClient = datasetClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    DatasetId instance = cliConfig.getCurrentNamespace().dataset(arguments.get(ArgumentName.DATASET.toString()));
    DatasetMeta meta = datasetClient.get(instance);

    Table table = Table.builder()
      .setHeader("hive table", "spec", "type", "principal")
      .setRows(ImmutableList.of(meta), new RowMaker<DatasetMeta>() {
        @Override
        public List<?> makeRow(DatasetMeta object) {
          return Lists.newArrayList(object.getHiveTableName(), GSON.toJson(object.getSpec()),
                                    GSON.toJson(object.getType()), object.getOwnerPrincipal());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("describe dataset instance <%s>", ArgumentName.DATASET);
  }

  @Override
  public String getDescription() {
    return String.format("Describes %s",
                         Fragment.of(Article.A, ElementType.DATASET.getName()));
  }
}
