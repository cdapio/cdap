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
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Gets the metadata properties of an entity.
 */
public class GetMetadataPropertiesCommand extends AbstractCommand {

  private final MetadataClient client;

  @Inject
  public GetMetadataPropertiesCommand(CLIConfig cliConfig, MetadataClient client) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    EntityId entity = EntityId.fromString(arguments.get(ArgumentName.ENTITY.toString()));
    String scope = arguments.getOptional(ArgumentName.METADATA_SCOPE.toString());
    Map<String, String> properties = scope == null ? client.getProperties(entity.toId()) :
      client.getProperties(entity.toId(), MetadataScope.valueOf(scope.toUpperCase()));

    Table table = Table.builder()
      .setHeader("key", "value")
      .setRows(
        Iterables.transform(properties.entrySet(), new Function<Map.Entry<String, String>, List<String>>() {
          @Nullable
          @Override
          public List<String> apply(@Nullable Map.Entry<String, String> entry) {
            return Lists.newArrayList(entry.getKey(), entry.getValue());
          }
        })
      ).build();

    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get metadata-properties <%s> [scope <%s>]",
                         ArgumentName.ENTITY, ArgumentName.METADATA_SCOPE);
  }

  @Override
  public String getDescription() {
    return "Gets the metadata properties of an entity";
  }
}
