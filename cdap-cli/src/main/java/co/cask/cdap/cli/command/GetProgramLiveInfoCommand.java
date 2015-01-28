/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.AsciiTable;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.DistributedProgramLiveInfo;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;

import java.io.PrintStream;

/**
 * Gets the live info of a program.
 */
public class GetProgramLiveInfoCommand extends AbstractAuthCommand implements Categorized {

  private final ProgramClient programClient;
  private final ElementType elementType;

  protected GetProgramLiveInfoCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    if (programIdParts.length < 2) {
      throw new CommandInputError(this);
    }
    String appId = programIdParts[0];
    String programId = programIdParts[1];

    DistributedProgramLiveInfo liveInfo = programClient.getLiveInfo(appId, elementType.getProgramType(), programId);

    new AsciiTable<DistributedProgramLiveInfo>(
      new String[] { "app", "type", "id", "runtime", "yarn app id"},
      Lists.newArrayList(liveInfo),
      new RowMaker<DistributedProgramLiveInfo>() {
        @Override
        public Object[] makeRow(DistributedProgramLiveInfo object) {
          return new Object[] { object.getApp(), object.getType(), object.getId(), object.getRuntime(),
            object.getYarnAppId() };
        }
      }
    ).print(output);

    if (liveInfo.getContainers() != null) {
      new AsciiTable<Containers.ContainerInfo>(
        new String[] { "containers", "instance", "host", "container", "memory", "virtual cores", "debug port" },
        liveInfo.getContainers(),
        new RowMaker<Containers.ContainerInfo>() {
          @Override
          public Object[] makeRow(Containers.ContainerInfo object) {
            return new Object[] { "", object.getInstance(), object.getHost(), object.getContainer(),
              object.getMemory(), object.getVirtualCores(), object.getDebugPort() };
          }
        }
      ).print(output);
    }
  }

  @Override
  public String getPattern() {
    return String.format("get %s live <%s>", elementType.getName(), elementType.getArgumentName());
  }

  @Override
  public String getDescription() {
    return "Gets the live info of a " + elementType.getPrettyName();
  }

  @Override
  public String getCategory() {
    return elementType.getCommandCategory().getName();
  }
}
