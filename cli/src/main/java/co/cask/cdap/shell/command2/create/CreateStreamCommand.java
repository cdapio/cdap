/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.shell.command2.create;

import co.cask.cdap.client.StreamClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command2.Arguments;
import co.cask.cdap.shell.command2.Command;

import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Creates a stream.
 */
public class CreateStreamCommand implements Command {

  private final StreamClient streamClient;

  @Inject
  public CreateStreamCommand(StreamClient streamClient) {
    this.streamClient = streamClient;
  }

  @Override
  public void execute(Arguments args, PrintStream output) throws Exception {
    String streamId = args.get("new-stream-id");

    streamClient.create(streamId);
    output.printf("Successfully created stream with ID '%s'\n", streamId);
  }

  @Override
  public String getPattern() {
    return "create stream <new-stream-id>";
  }

  @Override
  public String getDescription() {
    return "Creates a " + ElementType.STREAM.getPrettyName();
  }

}
