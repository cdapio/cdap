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

package co.cask.cdap.shell.command.send;

import co.cask.cdap.client.StreamClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.ArgumentName;
import co.cask.cdap.shell.command.Arguments;
import co.cask.cdap.shell.command.Command;
import co.cask.cdap.shell.completer.element.StreamIdCompleter;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;

/**
 * Sends an event to a stream.
 */
public class SendStreamEventCommand extends AbstractCommand {

  private final StreamClient streamClient;

  @Inject
  public SendStreamEventCommand(StreamClient streamClient) {
    this.streamClient = streamClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String streamId = arguments.get(ArgumentName.STREAM);
    String streamEvent = arguments.getRawInput().substring(String.format("send stream %s ", streamId).length());
    streamClient.sendEvent(streamId, streamEvent);
    output.printf("Successfully send stream event to stream '%s'\n", streamId);
  }

  @Override
  public String getPattern() {
    return String.format("send stream <%s> <%s>", ArgumentName.STREAM, ArgumentName.STREAM_EVENT);
  }

  @Override
  public String getDescription() {
    return "Sends an event to a " + ElementType.STREAM.getPrettyName();
  }
}
