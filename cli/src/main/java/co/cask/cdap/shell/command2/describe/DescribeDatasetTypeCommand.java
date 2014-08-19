/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.shell.command2.describe;

import co.cask.cdap.client.DatasetTypeClient;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command2.Arguments;
import co.cask.cdap.shell.command2.Command;
import co.cask.cdap.shell.util.AsciiTable;
import co.cask.cdap.shell.util.RowMaker;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.PrintStream;
import java.util.List;
import javax.inject.Inject;

/**
 * Shows information about a dataset type.
 */
public class DescribeDatasetTypeCommand implements Command {

  private final DatasetTypeClient datasetTypeClient;

  @Inject
  public DescribeDatasetTypeCommand(DatasetTypeClient datasetTypeClient) {
    this.datasetTypeClient = datasetTypeClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String typeName = arguments.get("dataset-type-name");
    DatasetTypeMeta datasetTypeMeta = datasetTypeClient.get(typeName);

    new AsciiTable<DatasetTypeMeta>(
      new String[] { "name", "modules" }, Lists.newArrayList(datasetTypeMeta),
      new RowMaker<DatasetTypeMeta>() {
        @Override
        public Object[] makeRow(DatasetTypeMeta object) {
          return new Object[] { object.getName(), Joiner.on(", ").join(getModuleNames(object.getModules())) };
        }
      }).print(output);
  }

  @Override
  public String getPattern() {
    return "describe dataset type <dataset-type-name>";
  }

  @Override
  public String getDescription() {
    return "Shows information about a " + ElementType.DATASET_TYPE.getPrettyName();
  }

  private List<String> getModuleNames(List<DatasetModuleMeta> modules) {
    ImmutableList.Builder<String> moduleNames = ImmutableList.builder();
    for (DatasetModuleMeta module : modules) {
      moduleNames.add(module.getName());
    }
    return moduleNames.build();
  }
}
