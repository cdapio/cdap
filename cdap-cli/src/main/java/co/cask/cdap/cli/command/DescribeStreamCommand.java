/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.command;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.StreamProperties;
import co.cask.common.cli.Arguments;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Shows detailed information about a stream.
 */
public class DescribeStreamCommand extends AbstractAuthCommand {

  private final StreamClient streamClient;
  private final TableRenderer tableRenderer;

  @Inject
  public DescribeStreamCommand(StreamClient streamClient, CLIConfig cliConfig, TableRenderer tableRenderer) {
    super(cliConfig);
    this.streamClient = streamClient;
    this.tableRenderer = tableRenderer;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String streamId = arguments.get(ArgumentName.STREAM.toString());
    StreamProperties config = streamClient.getConfig(streamId);

    Table table = Table.builder()
      .setHeader("ttl", "format", "schema", "notification.threshold.mb")
      .setRows(ImmutableList.of(config), new RowMaker<StreamProperties>() {
        @Override
        public List<?> makeRow(StreamProperties object) {
          FormatSpecification format = object.getFormat();
          return Lists.newArrayList(object.getTTL(), format.getName(), format.getSchema().toString(),
                                    object.getNotificationThresholdMB());
        }
      }).build();
    tableRenderer.render(output, table);
  }

  @Override
  public String getPattern() {
    return String.format("describe stream <%s>", ArgumentName.STREAM);
  }

  @Override
  public String getDescription() {
    return String.format("Shows detailed information about a %s.", ElementType.STREAM.getPrettyName());
  }
}
