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

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists the schedules for a given workflow.
 */
public final class ListWorkflowSchedulesCommand extends AbstractCommand {

  private static final Gson GSON = new Gson();

  private final ScheduleClient scheduleClient;

  @Inject
  ListWorkflowSchedulesCommand(CLIConfig cliConfig, ScheduleClient scheduleClient) {
    super(cliConfig);
    this.scheduleClient = scheduleClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(ElementType.WORKFLOW.getArgumentName().toString()).split("\\.");
    if (programIdParts.length < 2) {
      throw new CommandInputError(this);
    }

    final String appId = programIdParts[0];
    String workflowName = programIdParts[1];
    WorkflowId workflowId = cliConfig.getCurrentNamespace().app(appId).workflow(workflowName);

    List<ScheduleSpecification> list = scheduleClient.list(workflowId);
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
    cliConfig.getTableRenderer().render(cliConfig, output, table);
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
      return String.format("stream: %s, trigger MB: %d",
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
    return String.format("Gets schedules of %s", Fragment.of(Article.A, ElementType.WORKFLOW.getName()));

  }
}
