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
package co.cask.cdap.notifyableworkflow;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.schedule.Schedules;

/**
 * Notify-able Workflow app.
 */
public class NotifyableWorkflowApp extends AbstractApplication<AppConfig> {
  @Override
  public void configure() {
    AppConfig appConfig = getConfig();
    setDescription("NotifyableWorkflowApp");
    addSpark(new ClassicSpark(appConfig.getMainClassName()));
    addWorkflow(new NotifyableWorkflow(appConfig.getNotificationEmailIds(), appConfig.getNotificationEmailSubject(),
                                       appConfig.getNotificationEmailBody()));

    int i = 0;
    for (String cron : appConfig.getSchedules()) {
      scheduleWorkflow(Schedules.builder("schedule" + i)
                         .setDescription("Notifyable Workflow schedule")
                         .createTimeSchedule(cron),
                       "NotifyableWorkflow");
    }
  }
}
