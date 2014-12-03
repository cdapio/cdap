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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ElementType;
import co.cask.cdap.client.ProgramClient;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.List;

/**
 * Contains commands for getting the runtime arguments of a program.
 */
public class GetProgramRuntimeArgsCommandSet extends CommandSet<Command> {

  @Inject
  public GetProgramRuntimeArgsCommandSet(ProgramClient programClient) {
    super(generateCommands(programClient));
  }

  public static List<Command> generateCommands(ProgramClient programClient) {
    List<Command> commands = Lists.newArrayList();
    for (ElementType elementType : ElementType.values()) {
      if (elementType.hasRuntimeArgs()) {
        commands.add(new GetProgramRuntimeArgsCommand(elementType, programClient));
      }
    }
    return commands;
  }
}
