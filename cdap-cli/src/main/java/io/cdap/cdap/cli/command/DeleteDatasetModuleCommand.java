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

import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.client.DatasetModuleClient;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.common.cli.Arguments;

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
    DatasetModuleId module = cliConfig.getCurrentNamespace().datasetModule(
      arguments.get(ArgumentName.DATASET_MODULE.toString()));

    datasetClient.delete(module);
    output.printf("Successfully deleted dataset module '%s'\n", module.getEntityName());
  }

  @Override
  public String getPattern() {
    return String.format("delete dataset module <%s>", ArgumentName.DATASET_MODULE);
  }

  @Override
  public String getDescription() {
    return String.format("Deletes %s", Fragment.of(Article.A, ElementType.DATASET_MODULE.getName()));
  }
}
