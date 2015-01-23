/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.cli.command.CallServiceCommand;
import co.cask.cdap.cli.command.ExecuteQueryCommand;
import co.cask.cdap.cli.command.GetServiceEndpointsCommand;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Default set of commands.
 */
public class DefaultCommands extends CommandSet<Command> {

  @Inject
  public DefaultCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(ExecuteQueryCommand.class))
        .add(injector.getInstance(CallServiceCommand.class))
        .add(injector.getInstance(GetServiceEndpointsCommand.class))
        .build(),
      ImmutableList.<CommandSet<Command>>builder()
        .add(injector.getInstance(BasicCommands.class))
        .add(injector.getInstance(ApplicationCommands.class))
        .add(injector.getInstance(StreamCommands.class))
        .add(injector.getInstance(ProgramCommands.class))
        .add(injector.getInstance(DatasetCommands.class))
        // TODO: remove procedures (?)
        .add(injector.getInstance(ProcedureCommands.class))
        .add(injector.getInstance(AdapterCommands.class))
        .add(injector.getInstance(PreferenceCommands.class))
      // TODO: uncomment when namespace is ready
//      .add(injector.getInstance(NamespaceCommands.class))
        .build());
  }
}
