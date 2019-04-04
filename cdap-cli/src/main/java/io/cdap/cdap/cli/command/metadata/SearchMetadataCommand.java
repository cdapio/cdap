/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.cli.command.metadata;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.MetadataClient;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.proto.metadata.MetadataSearchResultRecord;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.Set;

/**
 * Command for metadata search in CLI.
 */
public class SearchMetadataCommand extends AbstractCommand {

  private final MetadataClient metadataClient;

  @Inject
  public SearchMetadataCommand(CLIConfig cliConfig, MetadataClient metadataClient) {
    super(cliConfig);
    this.metadataClient = metadataClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String searchQuery = arguments.get(ArgumentName.SEARCH_QUERY.toString());
    String type = arguments.getOptional(ArgumentName.TARGET_TYPE.toString());
    MetadataSearchResponse metadataSearchResponse =
      metadataClient.searchMetadata(cliConfig.getCurrentNamespace(), searchQuery, parseTargetType(type),
                                    null, 0, Integer.MAX_VALUE, 0, null, false);
    Set<MetadataSearchResultRecord> searchResults = metadataSearchResponse.getResults();
    Table table = Table.builder()
      .setHeader("Entity")
      .setRows(Lists.newArrayList(searchResults), searchResult ->
        Lists.newArrayList(searchResult.getEntityId().toString())).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("search metadata <%s> [filtered by target-type <%s>]", ArgumentName.SEARCH_QUERY,
                         ArgumentName.TARGET_TYPE);
  }

  @Override
  public String getDescription() {
    return "Searches CDAP entities based on the metadata annotated on them. " +
      "The search can be restricted by adding a comma-separated list of target types: " +
      "'artifact', 'app', 'dataset', 'program', 'stream', or 'view'.";
  }

  private Set<String> parseTargetType(String typeString) {
    if (typeString == null) {
      return ImmutableSet.of();
    }
    return ImmutableSet.copyOf(Splitter.on(',').split(typeString));
  }
}
