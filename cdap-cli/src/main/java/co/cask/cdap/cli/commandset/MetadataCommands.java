/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.cli.commandset;

import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.command.metadata.AddMetadataPropertiesCommand;
import co.cask.cdap.cli.command.metadata.AddMetadataTagsCommand;
import co.cask.cdap.cli.command.metadata.GetMetadataCommand;
import co.cask.cdap.cli.command.metadata.GetMetadataPropertiesCommand;
import co.cask.cdap.cli.command.metadata.GetMetadataTagsCommand;
import co.cask.cdap.cli.command.metadata.RemoveMetadataCommand;
import co.cask.cdap.cli.command.metadata.RemoveMetadataPropertiesCommand;
import co.cask.cdap.cli.command.metadata.RemoveMetadataPropertyCommand;
import co.cask.cdap.cli.command.metadata.RemoveMetadataTagCommand;
import co.cask.cdap.cli.command.metadata.RemoveMetadataTagsCommand;
import co.cask.cdap.cli.command.metadata.SearchMetadataCommand;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Metadata commands.
 */
public class MetadataCommands extends CommandSet<Command> implements Categorized {

  @Inject
  public MetadataCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(AddMetadataPropertiesCommand.class))
        .add(injector.getInstance(AddMetadataTagsCommand.class))
        .add(injector.getInstance(GetMetadataCommand.class))
        .add(injector.getInstance(GetMetadataPropertiesCommand.class))
        .add(injector.getInstance(GetMetadataTagsCommand.class))
        .add(injector.getInstance(RemoveMetadataCommand.class))
        .add(injector.getInstance(RemoveMetadataPropertiesCommand.class))
        .add(injector.getInstance(RemoveMetadataPropertyCommand.class))
        .add(injector.getInstance(RemoveMetadataTagCommand.class))
        .add(injector.getInstance(RemoveMetadataTagsCommand.class))
        .add(injector.getInstance(SearchMetadataCommand.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.METADATA_AND_LINEAGE.getName();
  }
}
