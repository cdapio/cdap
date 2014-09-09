/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command.set;

import co.cask.cdap.client.StreamClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.ArgumentName;
import co.cask.cdap.shell.command.Arguments;
import co.cask.cdap.shell.command.Command;

import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Sets the Time-to-Live (TTL) of a stream.
 */
public class SetStreamTTLCommand extends AbstractCommand {

  private final StreamClient streamClient;

  @Inject
  public SetStreamTTLCommand(StreamClient streamClient) {
    this.streamClient = streamClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String streamId = arguments.get(ArgumentName.STREAM);
    long ttlInSeconds = arguments.getLong(ArgumentName.TTL_IN_SECONDS);
    streamClient.setTTL(streamId, ttlInSeconds);
    output.printf("Successfully set TTL of stream '%s' to %d\n", streamId, ttlInSeconds);
  }

  @Override
  public String getPattern() {
    return String.format("set stream ttl <%s> <%s>", ArgumentName.STREAM, ArgumentName.TTL_IN_SECONDS);
  }

  @Override
  public String getDescription() {
    return "Sets the Time-to-Live (TTL) of a " + ElementType.STREAM.getPrettyName();
  }
}
