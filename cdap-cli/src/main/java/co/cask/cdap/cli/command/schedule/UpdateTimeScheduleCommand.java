/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.ArgumentParser;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.proto.ScheduleInstanceConfiguration;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.common.cli.Arguments;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Map;

/**
 * Updates a schedule.
 */
public final class UpdateTimeScheduleCommand extends AbstractCommand {

  private final ScheduleClient scheduleClient;

  @Inject
  public UpdateTimeScheduleCommand(CLIConfig cliConfig, ScheduleClient scheduleClient) {
    super(cliConfig);
    this.scheduleClient = scheduleClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String scheduleName = arguments.get(ArgumentName.SCHEDULE_NAME.toString());
    String[] programIdParts = arguments.get(ArgumentName.PROGRAM.toString()).split("\\.");
    String version = arguments.getOptional(ArgumentName.APP_VERSION.toString());
    String scheduleDescription = arguments.getOptional(ArgumentName.DESCRIPTION.toString(), "");
    String cronExpression = arguments.get(ArgumentName.CRON_EXPRESSION.toString());
    String schedulePropertiesString = arguments.getOptional(ArgumentName.SCHEDULE_PROPERTIES.toString(), "");
    String scheduleRunConcurrencyString = arguments.getOptional(ArgumentName.CONCURRENCY.toString(), null);

    if (programIdParts.length < 2) {
      throw new CommandInputError(this);
    }

    String appId = programIdParts[0];
    NamespaceId namespaceId = cliConfig.getCurrentNamespace();
    ApplicationId applicationId = (version == null) ? namespaceId.app(appId) : namespaceId.app(appId, version);
    ScheduleId scheduleId = applicationId.schedule(scheduleName);

    Schedules.Builder builder = Schedules.builder(scheduleName);
    if (scheduleRunConcurrencyString != null) {
      builder.setMaxConcurrentRuns(Integer.valueOf(scheduleRunConcurrencyString));
    }
    if (scheduleDescription != null) {
      builder.setDescription(scheduleDescription);
    }
    Schedule schedule = builder.createTimeSchedule(cronExpression);

    Map<String, String> programMap = ImmutableMap.of("programName", programIdParts[1],
                                                     "programType", ElementType.WORKFLOW.name().toUpperCase());
    Map<String, String> propertiesMap = ArgumentParser.parseMap(schedulePropertiesString,
                                                                ArgumentName.SCHEDULE_PROPERTIES.toString());

    ScheduleInstanceConfiguration configuration =
      new ScheduleInstanceConfiguration("TIME", schedule, programMap, propertiesMap);

    scheduleClient.update(scheduleId, configuration);
    printStream.printf("Successfully updated schedule '%s' in app '%s'\n", scheduleName, appId);
  }

  @Override
  public String getPattern() {
    return String.format("update time schedule <%s> for workflow <%s> [version <%s>] " +
                           "[description <%s>] at <%s> [concurrency <%s>] [properties <%s>]",
                         ArgumentName.SCHEDULE_NAME, ArgumentName.PROGRAM, ArgumentName.APP_VERSION,
                         ArgumentName.DESCRIPTION, ArgumentName.CRON_EXPRESSION, ArgumentName.CONCURRENCY,
                         ArgumentName.SCHEDULE_PROPERTIES);
  }

  @Override
  public String getDescription() {
    return String.format("Updates %s which is associated with %s given",
                         Fragment.of(Article.A, ElementType.SCHEDULE.getName()),
                         Fragment.of(Article.THE, ElementType.PROGRAM.getName()));
  }
}
