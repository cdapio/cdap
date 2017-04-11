/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
package co.cask.cdap.cli.command.schedule;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Resumes a schedule.
 */
public final class ResumeScheduleCommand extends AbstractCommand {

  private final ScheduleClient scheduleClient;

  @Inject
  public ResumeScheduleCommand(CLIConfig cliConfig, ScheduleClient scheduleClient) {
    super(cliConfig);
    this.scheduleClient = scheduleClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String[] programIdParts = arguments.get(ElementType.SCHEDULE.getArgumentName().toString()).split("\\.");
    if (programIdParts.length < 2) {
      throw new CommandInputError(this);
    }

    String appId = programIdParts[0];
    String scheduleName = programIdParts[1];
    ScheduleId schedule = cliConfig.getCurrentNamespace().app(appId).schedule(scheduleName);

    scheduleClient.resume(schedule);
    printStream.printf("Successfully resumed schedule '%s' in app '%s'\n", scheduleName, appId);
  }

  @Override
  public String getPattern() {
    return String.format("resume schedule <%s>", ElementType.SCHEDULE.getArgumentName());
  }

  @Override
  public String getDescription() {
    return String.format("Resumes %s", Fragment.of(Article.A, ElementType.SCHEDULE.getName()));
  }
}
