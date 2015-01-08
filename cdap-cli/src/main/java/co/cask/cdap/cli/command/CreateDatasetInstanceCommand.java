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
import co.cask.cdap.client.DatasetClient;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Creates a dataset.
 */
public class CreateDatasetInstanceCommand extends AbstractAuthCommand {

  private final DatasetClient datasetClient;

  @Inject
  public CreateDatasetInstanceCommand(DatasetClient datasetClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.datasetClient = datasetClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String datasetType = arguments.get(ArgumentName.DATASET_TYPE.toString());
    String datasetName = arguments.get(ArgumentName.NEW_DATASET.toString());

    datasetClient.create(datasetName, datasetType);
    output.printf("Successfully created dataset named '%s' with type '%s'\n", datasetName, datasetType);
  }

  @Override
  public String getPattern() {
    return String.format("create dataset instance <%s> <%s>", ArgumentName.DATASET_TYPE, ArgumentName.NEW_DATASET);
  }

  @Override
  public String getDescription() {
    return "Creates a " + ElementType.DATASET.getPrettyName();
  }
}
