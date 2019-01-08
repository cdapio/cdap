/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Gets the metadata of an entity.
 */
public class GetMetadataCommand extends AbstractCommand {

  private final MetadataClient client;

  @Inject
  public GetMetadataCommand(CLIConfig cliConfig, MetadataClient client) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    MetadataEntity metadataEntity =
      MetadataCommandHelper.toMetadataEntity(arguments.get(ArgumentName.ENTITY.toString()));
    String scope = arguments.getOptional(ArgumentName.METADATA_SCOPE.toString());
    Set<MetadataRecord> metadata = scope == null ? client.getMetadata(metadataEntity) :
      client.getMetadata(metadataEntity, MetadataScope.valueOf(scope.toUpperCase()));

    Table table = getTableBuilder().setRows(
      metadata.stream().map(record -> Lists.newArrayList(
        record.toString(),
        Joiner.on("\n").join(record.getTags()),
        Joiner.on("\n").withKeyValueSeparator(":").join(record.getProperties()),
        record.getScope().name())).collect(Collectors.toList())
    ).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  private Table.Builder getTableBuilder() {
    return Table.builder()
      .setHeader("entity", "tags", "properties", "scope");
  }

  @Override
  public String getPattern() {
    return String.format("get metadata <%s> [scope <%s>]",
                         ArgumentName.ENTITY, ArgumentName.METADATA_SCOPE);
  }

  @Override
  public String getDescription() {
    return "Gets the metadata of an entity. " + ArgumentName.ENTITY_DESCRIPTION_STRING;
  }
}
