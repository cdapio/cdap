/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.cli.commandset;

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Injector;

import java.io.PrintStream;
import java.util.List;

/**
 * Schedule commands.
 */
public class ScheduleCommands extends CommandSet<Command> implements Categorized {

  private static final Gson GSON = new Gson();

  @Inject
  public ScheduleCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(GetScheduleStatusCommand.class))
        .add(injector.getInstance(SuspendScheduleCommand.class))
        .add(injector.getInstance(ResumeScheduleCommand.class))
        .add(injector.getInstance(ListWorkflowSchedulesCommand.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.LIFECYCLE.getName();
  }

  /**
   * Gets the status of a schedule.
   */
  public static final class GetScheduleStatusCommand extends AbstractCommand {

    private final ScheduleClient scheduleClient;

    @Inject
    public GetScheduleStatusCommand(CLIConfig cliConfig, ScheduleClient scheduleClient) {
      super(cliConfig);
      this.scheduleClient = scheduleClient;
    }

    @Override
    public void perform(Arguments arguments, PrintStream printStream) throws Exception {
      String[] programIdParts = arguments.get("app-id.schedule-id").split("\\.");
      if (programIdParts.length < 2) {
        throw new CommandInputError(this);
      }

      String appId = programIdParts[0];
      String scheduleId = programIdParts[1];
      printStream.println(scheduleClient.getStatus(appId, scheduleId));
    }

    @Override
    public String getPattern() {
      return "get schedule status <app-id.schedule-id>";
    }

    @Override
    public String getDescription() {
      return String.format("Gets the status of a schedule");
    }
  }

  /**
   * Suspends a schedule.
   */
  public static final class SuspendScheduleCommand extends AbstractCommand {

    private final ScheduleClient scheduleClient;

    @Inject
    public SuspendScheduleCommand(CLIConfig cliConfig, ScheduleClient scheduleClient) {
      super(cliConfig);
      this.scheduleClient = scheduleClient;
    }

    @Override
    public void perform(Arguments arguments, PrintStream printStream) throws Exception {
      String[] programIdParts = arguments.get("app-id.schedule-id").split("\\.");
      if (programIdParts.length < 2) {
        throw new CommandInputError(this);
      }

      String appId = programIdParts[0];
      String scheduleId = programIdParts[1];
      scheduleClient.suspend(appId, scheduleId);
      printStream.printf("Successfully suspended schedule '%s' in app '%s'", scheduleId, appId);
    }

    @Override
    public String getPattern() {
      return "suspend schedule <app-id.schedule-id>";
    }

    @Override
    public String getDescription() {
      return "Suspends a schedule";
    }
  }

  /**
   * Resumes a schedule.
   */
  public static final class ResumeScheduleCommand extends AbstractCommand {

    private final ScheduleClient scheduleClient;

    @Inject
    public ResumeScheduleCommand(CLIConfig cliConfig, ScheduleClient scheduleClient) {
      super(cliConfig);
      this.scheduleClient = scheduleClient;
    }

    @Override
    public void perform(Arguments arguments, PrintStream printStream) throws Exception {
      String[] programIdParts = arguments.get("app-id.schedule-id").split("\\.");
      if (programIdParts.length < 2) {
        throw new CommandInputError(this);
      }

      String appId = programIdParts[0];
      String scheduleId = programIdParts[1];
      scheduleClient.resume(appId, scheduleId);
      printStream.printf("Successfully resumed schedule '%s' in app '%s'", scheduleId, appId);
    }

    @Override
    public String getPattern() {
      return "resume schedule <app-id.schedule-id>";
    }

    @Override
    public String getDescription() {
      return "Resumes a schedule";
    }
  }

  /**
   * Lists the schedules for a given workflow.
   */
  private static final class ListWorkflowSchedulesCommand extends AbstractCommand {

    private final ScheduleClient scheduleClient;
    private final TableRenderer tableRenderer;

    @Inject
    public ListWorkflowSchedulesCommand(CLIConfig cliConfig, ScheduleClient scheduleClient,
                                        TableRenderer tableRenderer) {
      super(cliConfig);
      this.scheduleClient = scheduleClient;
      this.tableRenderer = tableRenderer;
    }

    @Override
    public void perform(Arguments arguments, PrintStream printStream) throws Exception {
      String[] programIdParts = arguments.get(ElementType.WORKFLOW.getArgumentName().toString()).split("\\.");
      if (programIdParts.length < 2) {
        throw new CommandInputError(this);
      }

      final String appId = programIdParts[0];
      String scheduleId = programIdParts[1];
      List<ScheduleSpecification> list = scheduleClient.list(appId, scheduleId);
      Table table = Table.builder()
        .setHeader("application", "program", "program type", "name", "type", "description", "properties",
                   "runtime args")
        .setRows(list, new RowMaker<ScheduleSpecification>() {
          @Override
          public List<?> makeRow(ScheduleSpecification object) {
            return Lists.newArrayList(appId,
                                      object.getProgram().getProgramName(),
                                      object.getProgram().getProgramType().name(),
                                      object.getSchedule().getName(),
                                      getScheduleType(object.getSchedule()),
                                      object.getSchedule().getDescription(),
                                      getScheduleProperties(object.getSchedule()),
                                      GSON.toJson(object.getProperties()));
          }
        }).build();
      tableRenderer.render(printStream, table);
    }

    private String getScheduleType(Schedule schedule) {
      return schedule.getClass().getName();
    }

    private String getScheduleProperties(Schedule schedule) {
      if (schedule instanceof TimeSchedule) {
        TimeSchedule timeSchedule = (TimeSchedule) schedule;
        return String.format("cron entry: %s", timeSchedule.getCronEntry());
      } else if (schedule instanceof StreamSizeSchedule) {
        StreamSizeSchedule streamSizeSchedule = (StreamSizeSchedule) schedule;
        return String.format("stream: %s trigger MB: %d",
                             streamSizeSchedule.getStreamName(), streamSizeSchedule.getDataTriggerMB());
      } else {
        return "";
      }
    }

    @Override
    public String getPattern() {
      return String.format("get %s schedules <%s>",
                           ElementType.WORKFLOW.getName(), ElementType.WORKFLOW.getArgumentName());
    }

    @Override
    public String getDescription() {
      return "Resumes a schedule";
    }
  }
}
