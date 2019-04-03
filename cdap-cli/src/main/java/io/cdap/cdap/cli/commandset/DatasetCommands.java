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

package io.cdap.cdap.cli.commandset;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.cli.Categorized;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.cdap.cli.command.CreateDatasetInstanceCommand;
import io.cdap.cdap.cli.command.DeleteDatasetInstanceCommand;
import io.cdap.cdap.cli.command.DeleteDatasetModuleCommand;
import io.cdap.cdap.cli.command.DeployDatasetModuleCommand;
import io.cdap.cdap.cli.command.DescribeDatasetInstanceCommand;
import io.cdap.cdap.cli.command.DescribeDatasetModuleCommand;
import io.cdap.cdap.cli.command.DescribeDatasetTypeCommand;
import io.cdap.cdap.cli.command.GetDatasetInstancePropertiesCommand;
import io.cdap.cdap.cli.command.ListDatasetInstancesCommand;
import io.cdap.cdap.cli.command.ListDatasetModulesCommand;
import io.cdap.cdap.cli.command.ListDatasetTypesCommand;
import io.cdap.cdap.cli.command.SetDatasetInstancePropertiesCommand;
import io.cdap.cdap.cli.command.TruncateDatasetInstanceCommand;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

/**
 * Dataset commands.
 */
public class DatasetCommands extends CommandSet<Command> implements Categorized {

  @Inject
  public DatasetCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(ListDatasetInstancesCommand.class))
        .add(injector.getInstance(ListDatasetModulesCommand.class))
        .add(injector.getInstance(ListDatasetTypesCommand.class))
        .add(injector.getInstance(DescribeDatasetInstanceCommand.class))
        .add(injector.getInstance(GetDatasetInstancePropertiesCommand.class))
        .add(injector.getInstance(SetDatasetInstancePropertiesCommand.class))
        .add(injector.getInstance(CreateDatasetInstanceCommand.class))
        .add(injector.getInstance(DeleteDatasetInstanceCommand.class))
        .add(injector.getInstance(TruncateDatasetInstanceCommand.class))
        .add(injector.getInstance(DescribeDatasetModuleCommand.class))
        .add(injector.getInstance(DeployDatasetModuleCommand.class))
        .add(injector.getInstance(DeleteDatasetModuleCommand.class))
        .add(injector.getInstance(DescribeDatasetTypeCommand.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.DATASET.getName();
  }
}
