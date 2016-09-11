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
 * the License
 */

package co.cask.cdap.cli.command.lineage;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.LineageClient;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

/**
 * Gets the lineage for a stream.
 */
public class GetStreamLineageCommand extends AbstractCommand {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private final LineageClient client;

  @Inject
  public GetStreamLineageCommand(CLIConfig cliConfig, LineageClient client) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    long currentTime = System.currentTimeMillis();
    StreamId stream = cliConfig.getCurrentNamespace().stream(arguments.get(ArgumentName.STREAM.toString()));
    long start = getTimestamp(arguments.getOptional("start", "min"), currentTime);
    long end = getTimestamp(arguments.getOptional("end", "max"), currentTime);
    Integer levels = arguments.getIntOptional("levels", null);

    LineageRecord lineage = client.getLineage(stream.toId(), start, end, levels);
    Table table = Table.builder()
      .setHeader("start", "end", "relations", "programs", "data")
      .setRows(
        Collections.<List<String>>singletonList(
          Lists.newArrayList(
            Long.toString(lineage.getStart()), Long.toString(lineage.getEnd()), GSON.toJson(lineage.getRelations()),
            GSON.toJson(lineage.getPrograms()), GSON.toJson(lineage.getData()))
        )
      ).build();

    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get lineage stream <%s> [start <start>] [end <end>] [levels <levels>]", ArgumentName.STREAM);
  }

  @Override
  public String getDescription() {
    return "Gets the lineage of a stream";
  }
}
