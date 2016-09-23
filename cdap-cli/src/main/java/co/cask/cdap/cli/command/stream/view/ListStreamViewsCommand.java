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

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.proto.id.StreamId;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists all views.
 */
public class ListStreamViewsCommand extends AbstractAuthCommand {

  private final Provider<StreamViewClient> client;

  @Inject
  ListStreamViewsCommand(Provider<StreamViewClient> client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    StreamId streamId = cliConfig.getCurrentNamespace().stream(arguments.get(ArgumentName.STREAM.toString()));
    List<String> views = client.get().list(streamId.toId());

    Table table = Table.builder()
      .setHeader("id")
      .setRows(views, new RowMaker<String>() {
        @Override
        public List<?> makeRow(String object) {
          return Lists.newArrayList(object);
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("list stream-views <%s>", ArgumentName.STREAM);
  }

  @Override
  public String getDescription() {
    return String.format("Lists all stream-%s", ElementType.VIEW.getNamePlural());
  }
}
