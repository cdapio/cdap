/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
 * the License
 */

package co.cask.cdap.cli.command.metadata;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.common.cli.Arguments;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Gets the metadata tags of an entity.
 */
public class GetMetadataTagsCommand extends AbstractCommand {

  private final MetadataClient client;

  @Inject
  public GetMetadataTagsCommand(CLIConfig cliConfig, MetadataClient client) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    EntityId entity = EntityId.fromString(arguments.get(ArgumentName.ENTITY.toString()));
    String scope = arguments.getOptional(ArgumentName.METADATA_SCOPE.toString());
    Set<String> tags = scope == null ? client.getTags(entity.toId()) :
      client.getTags(entity.toId(), MetadataScope.valueOf(scope.toUpperCase()));

    Table table = Table.builder()
      .setHeader("tags")
      .setRows(
        Iterables.transform(tags, new Function<String, List<String>>() {
          @Nullable
          @Override
          public List<String> apply(@Nullable String tag) {
            return Lists.newArrayList(tag);
          }
        })
      ).build();

    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get metadata-tags <%s> [scope <%s>]",
                         ArgumentName.ENTITY, ArgumentName.METADATA_SCOPE);
  }

  @Override
  public String getDescription() {
    return "Gets the metadata tags of an entity. " + ArgumentName.ENTITY_DESCRIPTION_STRING;
  }
}
