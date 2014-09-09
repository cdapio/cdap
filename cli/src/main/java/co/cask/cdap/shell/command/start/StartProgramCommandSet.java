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

package co.cask.cdap.shell.command.start;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.CommandSet;
import co.cask.cdap.shell.command.HasCommand;
import com.google.common.collect.Lists;

import java.util.List;
import javax.inject.Inject;

/**
 * Contains commands for getting the number of instances a program is running on.
 */
public class StartProgramCommandSet extends CommandSet {

  @Inject
  public StartProgramCommandSet(ProgramClient programClient) {
    super(generateCommands(programClient));
  }

  public static List<HasCommand> generateCommands(ProgramClient programClient) {
    List<HasCommand> commands = Lists.newArrayList();
    for (ElementType elementType : ElementType.values()) {
      if (elementType.canStartStop()) {
        commands.add(new StartProgramCommand(elementType, programClient));
      }
    }
    return commands;
  }
}
