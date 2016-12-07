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

package co.cask.cdap.cli.command.stream.view;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.proto.id.StreamId;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

/**
 * Shows detailed information about a stream view.
 */
public class DescribeStreamViewCommand extends AbstractAuthCommand {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private final StreamViewClient client;

  @Inject
  public DescribeStreamViewCommand(StreamViewClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    StreamId streamId = cliConfig.getCurrentNamespace().stream(arguments.get(ArgumentName.STREAM.toString()));
    ViewDetail detail = client.get(streamId.view(arguments.get(ArgumentName.VIEW.toString())));

    Table table = Table.builder()
      .setHeader("id", "format", "table", "schema", "settings")
      .setRows(Collections.singletonList(detail), new RowMaker<ViewDetail>() {
        @Override
        public List<?> makeRow(ViewDetail object) {
          return Lists.newArrayList(object.getId(), object.getFormat().getName(),
                                    object.getTableName(),
                                    GSON.toJson(object.getFormat().getSchema()),
                                    GSON.toJson(object.getFormat().getSettings()));
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("describe stream-view <%s> <%s>", ArgumentName.STREAM, ArgumentName.VIEW);
  }

  @Override
  public String getDescription() {
    return String.format("Describes a stream-%s", ElementType.VIEW.getName());
  }
}
