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

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.DatasetModuleClient;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Deletes a dataset module.
 */
public class DeleteDatasetModuleCommand extends AbstractAuthCommand {

  private final DatasetModuleClient datasetClient;

  @Inject
  public DeleteDatasetModuleCommand(DatasetModuleClient datasetClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.datasetClient = datasetClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String datasetModuleName = arguments.get(ArgumentName.DATASET_MODULE.toString());

    datasetClient.delete(datasetModuleName);
    output.printf("Successfully deleted dataset module '%s'\n", datasetModuleName);
  }

  @Override
  public String getPattern() {
    return String.format("delete dataset module <%s>", ArgumentName.DATASET_MODULE);
  }

  @Override
  public String getDescription() {
    return "Deletes a " + ElementType.DATASET_MODULE.getPrettyName();
  }
}
