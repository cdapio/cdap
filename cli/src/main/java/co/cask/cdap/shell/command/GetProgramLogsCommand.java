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
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.ProgramIdCompleterFactory;
import co.cask.cdap.shell.completer.Completable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.List;

/**
 * Gets the logs of a program.
 */
public class GetProgramLogsCommand extends AbstractCommand implements Completable {

  private final ProgramClient programClient;
  private final ProgramIdCompleterFactory completerFactory;
  private final ElementType elementType;

  protected GetProgramLogsCommand(ElementType elementType,
                                  ProgramIdCompleterFactory completerFactory,
                                  ProgramClient programClient) {
    super(elementType.getName(), "<app-id>.<program-id> [<start-time> <end-time>]",
          "Gets the logs of a " + elementType.getPrettyName());
    this.elementType = elementType;
    this.completerFactory = completerFactory;
    this.programClient = programClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String[] programIdParts = args[0].split("\\.");
    String appId = programIdParts[0];

    long start;
    long stop;
    if (args.length >= 4) {
      start = Long.parseLong(args[2]);
      stop = Long.parseLong(args[3]);
      Preconditions.checkArgument(stop >= start, "stop timestamp must be greater than start timestamp");
    } else {
      start = 0;
      stop = Long.MAX_VALUE;
    }

    String logs;
    if (elementType == ElementType.RUNNABLE) {
      String serviceId = programIdParts[1];
      String runnableId = programIdParts[2];
      logs = programClient.getServiceRunnableLogs(appId, serviceId, runnableId, start, stop);
    } else if (elementType.getProgramType() != null) {
      String programId = programIdParts[1];
      logs = programClient.getProgramLogs(appId, elementType.getProgramType(), programId, start, stop);
    } else {
      throw new IllegalArgumentException("Cannot get logs for " + elementType.getPluralName());
    }

    output.println(logs);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completerFactory.getProgramIdCompleter(elementType)));
  }
}
