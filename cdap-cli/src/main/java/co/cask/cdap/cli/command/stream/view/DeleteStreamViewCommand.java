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
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Deletes a stream view.
 */
public class DeleteStreamViewCommand extends AbstractAuthCommand {

  private final StreamViewClient client;

  @Inject
  public DeleteStreamViewCommand(StreamViewClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    StreamId streamId = cliConfig.getCurrentNamespace().stream(arguments.get(ArgumentName.STREAM.toString()));
    StreamViewId view = streamId.view(arguments.get(ArgumentName.VIEW.toString()));
    client.delete(view.toId());
    output.printf("Successfully deleted stream-view '%s'\n", view.getEntityName());
  }

  @Override
  public String getPattern() {
    return String.format("delete stream-view <%s> <%s>", ArgumentName.STREAM, ArgumentName.VIEW);
  }

  @Override
  public String getDescription() {
    return String.format("Deletes a stream-%s", ElementType.VIEW.getName());
  }
}
