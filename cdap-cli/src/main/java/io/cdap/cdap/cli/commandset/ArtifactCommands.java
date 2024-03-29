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
import io.cdap.cdap.cli.command.artifact.DeleteArtifactCommand;
import io.cdap.cdap.cli.command.artifact.DescribeArtifactCommand;
import io.cdap.cdap.cli.command.artifact.DescribeArtifactPluginCommand;
import io.cdap.cdap.cli.command.artifact.GetArtifactPropertiesCommand;
import io.cdap.cdap.cli.command.artifact.ListArtifactPluginTypesCommand;
import io.cdap.cdap.cli.command.artifact.ListArtifactPluginsCommand;
import io.cdap.cdap.cli.command.artifact.ListArtifactVersionsCommand;
import io.cdap.cdap.cli.command.artifact.ListArtifactsCommand;
import io.cdap.cdap.cli.command.artifact.LoadArtifactCommand;
import io.cdap.cdap.cli.command.artifact.SetArtifactPropertiesCommand;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

/**
 * Artifact commands.
 */
public class ArtifactCommands extends CommandSet<Command> implements Categorized {

  @Inject
  public ArtifactCommands(Injector injector) {
    super(
        ImmutableList.<Command>builder()
            .add(injector.getInstance(DeleteArtifactCommand.class))
            .add(injector.getInstance(DescribeArtifactCommand.class))
            .add(injector.getInstance(DescribeArtifactPluginCommand.class))
            .add(injector.getInstance(GetArtifactPropertiesCommand.class))
            .add(injector.getInstance(ListArtifactsCommand.class))
            .add(injector.getInstance(ListArtifactPluginTypesCommand.class))
            .add(injector.getInstance(ListArtifactPluginsCommand.class))
            .add(injector.getInstance(ListArtifactVersionsCommand.class))
            .add(injector.getInstance(LoadArtifactCommand.class))
            .add(injector.getInstance(SetArtifactPropertiesCommand.class))
            .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.ARTIFACT.getName();
  }
}
