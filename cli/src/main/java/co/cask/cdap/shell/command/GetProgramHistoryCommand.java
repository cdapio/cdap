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
import co.cask.cdap.proto.RunRecord;
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
 * Gets the run history of a program.
 */
public class GetProgramHistoryCommand extends AbstractCommand implements Completable {

  private final ProgramClient programClient;
  private final ProgramIdCompleterFactory completerFactory;
  private final ElementType elementType;

  protected GetProgramHistoryCommand(ElementType elementType,
                                     ProgramIdCompleterFactory completerFactory,
                                     ProgramClient programClient) {
    super(elementType.getName(), "<app-id>.<program-id>",
          "Gets the run history of a " + elementType.getPrettyName());
    this.elementType = elementType;
    this.completerFactory = completerFactory;
    this.programClient = programClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String[] programIdParts = args[0].split("\\.");
    String appId = programIdParts[0];

    List<RunRecord> history;
    if (elementType == ElementType.RUNNABLE) {
      String serviceId = programIdParts[1];
      String runnableId = programIdParts[2];
      history = programClient.getServiceRunnableHistory(appId, serviceId, runnableId);
    } else if (elementType.getProgramType() != null) {
      String programId = programIdParts[1];
      history = programClient.getProgramHistory(appId, elementType.getProgramType(), programId);
    } else {
      throw new IllegalArgumentException("Unrecognized program element type for history: " + elementType);
    }

    new AsciiTable<RunRecord>(
      new String[] { "pid", "end status", "start", "stop" },
      history,
      new RowMaker<RunRecord>() {
        @Override
        public Object[] makeRow(RunRecord object) {
          return new Object[] { object.getPid(), object.getEndStatus(), object.getStartTs(), object.getStopTs() };
        }
      }
    ).print(output);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completerFactory.getProgramIdCompleter(elementType)));
  }
}
