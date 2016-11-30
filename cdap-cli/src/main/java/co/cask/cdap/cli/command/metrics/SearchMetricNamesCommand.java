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
import co.cask.cdap.client.MetricsClient;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Searches metric names.
 */
public class SearchMetricNamesCommand extends AbstractAuthCommand {

  private final MetricsClient client;

  @Inject
  public SearchMetricNamesCommand(MetricsClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    Map<String, String> tags = ArgumentParser.parseMap(arguments.getOptional("tags", ""));
    List<String> results = client.searchMetrics(tags);
    for (String result : results) {
      output.println(result);
    }
  }

  @Override
  public String getPattern() {
    return "search metric names [<tags>]";
  }

  @Override
  public String getDescription() {
    return "Searches metric names. Provide '<tags>' as a map in the format 'tag1=value1 tag2=value2'.";
  }
}
