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
package co.cask.cdap.cli.command.dataset;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Executes a method on a dataset.
 */
public class ExecuteDatasetMethodCommand extends AbstractAuthCommand {

  private final DatasetClient client;

  @Inject
  public ExecuteDatasetMethodCommand(CLIConfig cliConfig, DatasetClient client) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String name = arguments.get(ArgumentName.DATASET.getName());
    Id.DatasetInstance instance = Id.DatasetInstance.from(cliConfig.getCurrentNamespace(), name);

    String method = arguments.get(ArgumentName.DATASET_METHOD.getName());
    String body = arguments.getOptional(ArgumentName.BODY.getName(), null);

    try {
      byte[] response = client.execute(instance, method, body);
      if (response == null || response.length == 0) {
        output.printf("Successfully executed method %s on dataset %s\n", method, instance.getId());
      } else {
        output.println(Bytes.toString(response));
      }
    } catch (NotFoundException e) {
      output.printf("Dataset instance %s was not found\n", instance.getId());
    }
  }

  @Override
  public String getPattern() {
    return String.format("execute dataset <%s> method <%s> [body <%s>]",
                         ArgumentName.DATASET, ArgumentName.DATASET_METHOD, ArgumentName.BODY);
  }

  @Override
  public String getDescription() {
    return "Executes a method on a dataset instance";
  }
}
