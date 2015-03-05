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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.StreamProperties;
import co.cask.common.cli.Arguments;

import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Sets the Notification Threshold of a Stream.
 */
public class SetStreamNotificationThresholdCommand extends AbstractAuthCommand {

  private final StreamClient streamClient;

  @Inject
  public SetStreamNotificationThresholdCommand(StreamClient streamClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.streamClient = streamClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String streamId = arguments.get(ArgumentName.STREAM.toString());
    int notificationThresholdMB = arguments.getInt(ArgumentName.NOTIFICATION_THRESHOLD_MB.toString());
    streamClient.setStreamProperties(streamId, new StreamProperties(null, null, notificationThresholdMB));
    output.printf("Successfully set notification threshold of stream '%s' to %dMB\n",
                  streamId, notificationThresholdMB);
  }

  @Override
  public String getPattern() {
    return String.format("set stream notification-threshold <%s> <%s>",
                         ArgumentName.STREAM, ArgumentName.NOTIFICATION_THRESHOLD_MB);
  }

  @Override
  public String getDescription() {
    return String.format("Sets the Notification Threshold of a %s.", ElementType.STREAM.getPrettyName());
  }
}
