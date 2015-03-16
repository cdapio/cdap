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
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.DistributedProgramLiveInfo;
import co.cask.common.cli.Arguments;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.PrintStream;
import java.util.List;

/**
 * Gets the live info of a program.
 */
public class GetProgramLiveInfoCommand extends AbstractAuthCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;
  private final TableRenderer tableRenderer;

  protected GetProgramLiveInfoCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig,
                                      TableRenderer tableRenderer) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
    this.tableRenderer = tableRenderer;
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

    if (liveInfo == null) {
      output.println("No live info found");
      return;
    }

    Table table = Table.builder()
      .setHeader("app", "type", "id", "runtime", "yarn app id")
      .setRows(ImmutableList.of(liveInfo), new RowMaker<DistributedProgramLiveInfo>() {
        @Override
        public List<?> makeRow(DistributedProgramLiveInfo object) {
          return Lists.newArrayList(object.getApp(), object.getType(), object.getName(),
                                    object.getRuntime(), object.getYarnAppId());
        }
      }).build();
    tableRenderer.render(output, table);

    if (liveInfo.getContainers() != null) {
      Table containersTable = Table.builder()
        .setHeader("containers", "instance", "host", "container", "memory", "virtual cores", "debug port")
        .setRows(liveInfo.getContainers(), new RowMaker<Containers.ContainerInfo>() {
          @Override
          public List<?> makeRow(Containers.ContainerInfo object) {
            return Lists.newArrayList("", object.getInstance(), object.getHost(), object.getContainer(),
              object.getMemory(), object.getVirtualCores(), object.getDebugPort());
          }
        }).build();
      tableRenderer.render(output, containersTable);
    }
  }

  @Override
  public String getPattern() {
    return String.format("get %s live <%s>", elementType.getName(), elementType.getArgumentName());
  }

  @Override
  public String getDescription() {
    return String.format("Gets the live info of a %s.", elementType.getPrettyName());
  }
}
