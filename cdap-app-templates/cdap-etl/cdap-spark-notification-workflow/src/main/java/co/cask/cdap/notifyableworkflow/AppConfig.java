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

import co.cask.cdap.api.Config;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.Set;

/**
 * Config used to create the notifyable workflow app
 */
public class AppConfig extends Config {
  private final String mainClassName;
  private final ETLPlugin etlPlugin;
  private final Set<String> schedules;
  private final Set<String> notificationEmailIds;
  private final String notificationEmailSubject;
  private final String notificationEmailBody;

  public AppConfig(String mainClassName, ETLPlugin etlPlugin, Set<String> schedules, Set<String> notificationEmailIds,
                   String notificationEmailSubject, String notificationEmailBody) {
    this.mainClassName = mainClassName;
    this.etlPlugin = etlPlugin;
    this.schedules = schedules;
    this.notificationEmailIds = notificationEmailIds;
    this.notificationEmailSubject = notificationEmailSubject;
    this.notificationEmailBody = notificationEmailBody;
  }

  public String getMainClassName() {
    return mainClassName;
  }

  public ETLPlugin getEtlPlugin() {
    return etlPlugin;
  }

  public Set<String> getSchedules() {
    return schedules;
  }

  public Set<String> getNotificationEmailIds() {
    return notificationEmailIds;
  }

  public String getNotificationEmailSubject() {
    return notificationEmailSubject;
  }

  public String getNotificationEmailBody() {
    return notificationEmailBody;
  }
}
