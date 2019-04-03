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
package io.cdap.cdap.cli.command.schedule;

import com.google.inject.Inject;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.exception.CommandInputError;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.common.cli.Arguments;

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
