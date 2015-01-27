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

package co.cask.cdap.cli.commandset;

import co.cask.cdap.cli.command.CreateStreamCommand;
import co.cask.cdap.cli.command.DescribeStreamCommand;
import co.cask.cdap.cli.command.GetStreamEventsCommand;
import co.cask.cdap.cli.command.ListStreamsCommand;
import co.cask.cdap.cli.command.LoadStreamCommand;
import co.cask.cdap.cli.command.SendStreamEventCommand;
import co.cask.cdap.cli.command.SetStreamTTLCommand;
import co.cask.cdap.cli.command.TruncateStreamCommand;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Stream commands.
 */
public class StreamCommands extends CommandSet<Command> {

  @Inject
  public StreamCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(CreateStreamCommand.class))
        .add(injector.getInstance(DescribeStreamCommand.class))
        .add(injector.getInstance(GetStreamEventsCommand.class))
        .add(injector.getInstance(ListStreamsCommand.class))
        .add(injector.getInstance(SendStreamEventCommand.class))
        .add(injector.getInstance(LoadStreamCommand.class))
        .add(injector.getInstance(SetStreamTTLCommand.class))
        .add(injector.getInstance(TruncateStreamCommand.class))
        .build());
  }
}
