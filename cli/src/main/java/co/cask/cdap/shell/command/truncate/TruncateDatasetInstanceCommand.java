/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command.truncate;

import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.ArgumentName;
import co.cask.cdap.shell.command.Arguments;
import co.cask.cdap.shell.command.Command;

import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Truncates a dataset.
 */
public class TruncateDatasetInstanceCommand extends AbstractCommand {

  private final DatasetClient datasetClient;

  @Inject
  public TruncateDatasetInstanceCommand(DatasetClient datasetClient) {
    this.datasetClient = datasetClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String datasetName = arguments.get(ArgumentName.DATASET);
    datasetClient.truncate(datasetName);
    output.printf("Successfully truncated dataset '%s'\n", datasetName);
  }

  @Override
  public String getPattern() {
    return String.format("truncate dataset instance <%s>", ArgumentName.DATASET);
  }

  @Override
  public String getDescription() {
    return "Truncates a " + ElementType.DATASET.getPrettyName();
  }
}
