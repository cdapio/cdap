/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.id.StreamId;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * A CLI command for getting stream events.
 */
public class GetStreamEventsCommand extends AbstractCommand {

  private final StreamClient streamClient;

  @Inject
  public GetStreamEventsCommand(StreamClient streamClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.streamClient = streamClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    long currentTime = System.currentTimeMillis();

    StreamId streamId = cliConfig.getCurrentNamespace().stream(arguments.get(ArgumentName.STREAM.toString()));
    long startTime = getTimestamp(arguments.getOptional(ArgumentName.START_TIME.toString(), "min"), currentTime);
    long endTime = getTimestamp(arguments.getOptional(ArgumentName.END_TIME.toString(), "max"), currentTime);
    int limit = arguments.getIntOptional(ArgumentName.LIMIT.toString(), Integer.MAX_VALUE);

    // Get a list of stream events and prints it.
    List<StreamEvent> events = streamClient.getEvents(streamId.toId(), startTime, endTime,
                                                      limit, Lists.<StreamEvent>newArrayList());
    Table table = Table.builder()
      .setHeader("timestamp", "headers", "body size", "body")
      .setRows(events, new RowMaker<StreamEvent>() {
        @Override
        public List<?> makeRow(StreamEvent event) {
          long bodySize = event.getBody().remaining();
          return Lists.newArrayList(event.getTimestamp(),
                                    event.getHeaders().isEmpty() ? "" : formatHeader(event.getHeaders()),
                                    bodySize, getBody(event.getBody()));
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);

    output.printf("Fetched %d events from stream '%s'", events.size(), streamId.getEntityName());
    output.println();
  }

  @Override
  public String getPattern() {
    return String.format("get stream <%s> [<%s>] [<%s>] [<%s>]",
                         ArgumentName.STREAM, ArgumentName.START_TIME, ArgumentName.END_TIME, ArgumentName.LIMIT);
  }

  @Override
  public String getDescription() {
    return String.format("Gets events from %s. The time format for '<%s>' and '<%s>' can be a timestamp in " +
      "milliseconds or a relative time in the form of '[+|-][0-9][d|h|m|s]'. '<%s>' is relative to current time; " + 
      "'<%s>' is relative to '<%s>'. Special constants 'min' and 'max' can be used to represent '0' and " +
      "'max timestamp' respectively.",
      Fragment.of(Article.A, ElementType.STREAM.getName()), ArgumentName.START_TIME, ArgumentName.END_TIME,
      ArgumentName.START_TIME, ArgumentName.END_TIME, ArgumentName.START_TIME);
  }

}
