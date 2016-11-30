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

package co.cask.cdap.cli.command.metrics;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.ArgumentParser;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Gets the value of a metric.
 */
public class GetMetricCommand extends AbstractAuthCommand {

  private final MetricsClient client;

  @Inject
  public GetMetricCommand(MetricsClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String metric = arguments.get("metric-name");
    Map<String, String> tags = ArgumentParser.parseMap(arguments.getOptional("tags", ""));
    String start = arguments.getOptional("start", "");
    String end = arguments.getOptional("end", "");

    MetricQueryResult result = client.query(
      tags, ImmutableList.of(metric), ImmutableList.<String>of(),
      start.isEmpty() ? null : start,
      end.isEmpty() ? null : end);

    output.printf("Start time: %d\n", result.getStartTime());
    output.printf("End time: %d\n", result.getEndTime());

    for (MetricQueryResult.TimeSeries series : result.getSeries()) {
      output.println();
      output.printf("Series: %s\n", series.getMetricName());
      if (!series.getGrouping().isEmpty()) {
        output.printf("Grouping: %s\n", Joiner.on(",").withKeyValueSeparator("=").join(series.getGrouping()));
      }
      Table table = Table.builder()
        .setHeader("timestamp", "value")
        .setRows(ImmutableList.copyOf(series.getData()), new RowMaker<MetricQueryResult.TimeValue>() {
          @Override
          public List<?> makeRow(MetricQueryResult.TimeValue object) {
            return Lists.newArrayList(object.getTime(), object.getValue());
          }
        }).build();
      cliConfig.getTableRenderer().render(cliConfig, output, table);
    }
  }

  @Override
  public String getPattern() {
    return "get metric value <metric-name> [<tags>] [start <start>] [end <end>]";
  }

  @Override
  public String getDescription() {
    return "Gets the value of a metric. Provide '<tags>' as a map in the format 'tag1=value1 tag2=value2'.";
  }
}
