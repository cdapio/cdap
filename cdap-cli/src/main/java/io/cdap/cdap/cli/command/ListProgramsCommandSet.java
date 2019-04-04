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

package io.cdap.cdap.cli.command;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

import java.util.List;

/**
 * Contains commands for getting the number of instances a program is running on.
 */
public class ListProgramsCommandSet extends CommandSet<Command> {

  @Inject
  public ListProgramsCommandSet(ApplicationClient applicationClient, CLIConfig cliConfig) {
    super(generateCommands(applicationClient, cliConfig));
  }

  private static List<Command> generateCommands(ApplicationClient applicationClient, CLIConfig cliConfig) {
    List<Command> commands = Lists.newArrayList();
    for (ElementType elementType : ElementType.values()) {
      if (elementType.getProgramType() != null && elementType.getProgramType().isListable()) {
        commands.add(new ListProgramsCommand(elementType.getProgramType(), applicationClient, cliConfig));
      }
    }
    return commands;
  }
}
