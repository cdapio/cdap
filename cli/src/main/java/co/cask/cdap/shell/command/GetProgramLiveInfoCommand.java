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

package co.cask.cdap.shell.command;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.DistributedProgramLiveInfo;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.ProgramIdCompleterFactory;
import co.cask.cdap.shell.completer.Completable;
import co.cask.cdap.shell.util.AsciiTable;
import co.cask.cdap.shell.util.RowMaker;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.List;

/**
 * Gets the live info of a program.
 */
public class GetProgramLiveInfoCommand extends AbstractCommand implements Completable {

  private final ProgramClient programClient;
  private final ProgramIdCompleterFactory completerFactory;
  private final ElementType elementType;

  protected GetProgramLiveInfoCommand(ElementType elementType,
                                      ProgramIdCompleterFactory completerFactory,
                                      ProgramClient programClient) {
    super(elementType.getName(), "<app-id>.<program-id>",
          "Gets the live info of a " + elementType.getPrettyName());
    this.elementType = elementType;
    this.completerFactory = completerFactory;
    this.programClient = programClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String[] programIdParts = args[0].split("\\.");
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
            return new Object[] { "", object.getInstance(), object.getHost(), object.getContainer(), object.getMemory(),
              object.getVirtualCores(), object.getDebugPort() };
          }
        }
      ).print(output);
    }
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completerFactory.getProgramIdCompleter(elementType)));
  }
}
