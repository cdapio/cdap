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
import co.cask.cdap.client.DatasetModuleClient;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;

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
    String moduleName = arguments.get(ArgumentName.DATASET_MODULE.toString());
    DatasetModuleMeta datasetModuleMeta = datasetModuleClient.get(moduleName);

    new AsciiTable<DatasetModuleMeta>(
      new String[] { "name", "className", "jarLocation", "types", "usesModules", "usedByModules" },
      Lists.newArrayList(datasetModuleMeta),
      new RowMaker<DatasetModuleMeta>() {
        @Override
        public Object[] makeRow(DatasetModuleMeta object) {
          return new Object[] { object.getName(), object.getClassName(), object.getJarLocation(),
            Joiner.on(", ").join(object.getTypes()),
            Joiner.on(", ").join(object.getUsesModules()),
            Joiner.on(", ").join(object.getUsedByModules())
          };
        }
      }).print(output);
  }

  @Override
  public String getPattern() {
    return String.format("describe dataset module <%s>", ArgumentName.DATASET_MODULE);
  }

  @Override
  public String getDescription() {
    return String.format("Shows information about a %s.", ElementType.DATASET_MODULE.getPrettyName());
  }
}
