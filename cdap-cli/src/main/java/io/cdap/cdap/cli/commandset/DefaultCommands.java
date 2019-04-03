/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.cli.commandset;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.cli.command.ExecuteQueryCommand;
import io.cdap.cdap.cli.command.PreferencesCommandSet;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

/**
 * Default set of commands.
 */
public class DefaultCommands extends CommandSet<Command> {

  @Inject
  public DefaultCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(ExecuteQueryCommand.class))
        .build(),
      ImmutableList.<CommandSet<Command>>builder()
        .add(injector.getInstance(GeneralCommands.class))
        .add(injector.getInstance(MetricsCommands.class))
        .add(injector.getInstance(ApplicationCommands.class))
        .add(injector.getInstance(ArtifactCommands.class))
        .add(injector.getInstance(ProgramCommands.class))
        .add(injector.getInstance(DatasetCommands.class))
        .add(injector.getInstance(ServiceCommands.class))
        .add(injector.getInstance(PreferencesCommandSet.class))
        .add(injector.getInstance(NamespaceCommands.class))
        .add(injector.getInstance(ScheduleCommands.class))
        .add(injector.getInstance(SecurityCommands.class))
        .add(injector.getInstance(LineageCommands.class))
        .add(injector.getInstance(MetadataCommands.class))
        .build());
  }
}
