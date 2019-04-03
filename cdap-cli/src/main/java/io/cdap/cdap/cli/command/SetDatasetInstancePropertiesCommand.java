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

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.cli.util.ArgumentParser;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.Map;

/**
 * Sets properties for a dataset instance.
 */
public class SetDatasetInstancePropertiesCommand extends AbstractCommand {

  private static final Gson GSON = new Gson();
  private final DatasetClient datasetClient;

  @Inject
  public SetDatasetInstancePropertiesCommand(DatasetClient datasetClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.datasetClient = datasetClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    DatasetId instance = cliConfig.getCurrentNamespace().dataset(arguments.get(ArgumentName.DATASET.toString()));
    Map<String, String> properties = ArgumentParser.parseMap(arguments.get(ArgumentName.DATASET_PROPERTIES.toString()),
                                                             ArgumentName.DATASET_PROPERTIES.toString());
    datasetClient.updateExisting(instance, properties);
    output.printf("Successfully updated properties for dataset instance '%s' to %s",
                  instance.getEntityName(), GSON.toJson(properties));
  }

  @Override
  public String getPattern() {
    return String.format("set dataset instance properties <%s> <%s>",
                         ArgumentName.DATASET, ArgumentName.DATASET_PROPERTIES);
  }

  @Override
  public String getDescription() {
    return String.format("Sets properties for %s. '<%s>' is in the format 'key1=val1 key2=val2'.",
                         Fragment.of(Article.A, ElementType.DATASET.getName()),
                         ArgumentName.DATASET_PROPERTIES);
  }
}
