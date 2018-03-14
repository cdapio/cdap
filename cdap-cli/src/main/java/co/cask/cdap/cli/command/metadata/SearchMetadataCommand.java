/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.cli.command.metadata;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.common.cli.Arguments;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;

/**
 * Command for metadata search in CLI.
 */
public class SearchMetadataCommand extends AbstractCommand {

  private static final Function<String, EntityTypeSimpleName> STRING_TO_TARGET_TYPE =
    new Function<String, EntityTypeSimpleName>() {
      @Override
      public EntityTypeSimpleName apply(String input) {
        return EntityTypeSimpleName.valueOf(input.toUpperCase());
      }
    };

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
      metadataClient.searchMetadata(cliConfig.getCurrentNamespace(), searchQuery, parseTargetType(type));
    Set<MetadataSearchResultRecord> searchResults = metadataSearchResponse.getResults();
    Table table = Table.builder()
      .setHeader("Entity")
      .setRows(Lists.newArrayList(searchResults), new RowMaker<MetadataSearchResultRecord>() {
        @Override
        public List<?> makeRow(MetadataSearchResultRecord searchResult) {
          return Lists.newArrayList(searchResult.getEntityId().toString());
        }
      }).build();
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

  private Set<EntityTypeSimpleName> parseTargetType(String typeString) {
    if (typeString == null) {
      return ImmutableSet.of();
    }

    return ImmutableSet.copyOf(Iterables.transform(Splitter.on(',').split(typeString), STRING_TO_TARGET_TYPE));
  }
}
