/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol.operationrunner;

import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import java.util.List;
import javax.annotation.Nullable;

public class PullAppResponse<T> {

  private final String applicationName;
  private final String applicationFileHash;
  private final AppRequest<T> appRequest;
  @Nullable
  private final PreferencesDetail preferences;
  @Nullable
  private final List<ScheduleDetail> schedules;

  public PullAppResponse(String applicationName, String applicationFileHash,
      AppRequest<T> appRequest, @Nullable PreferencesDetail preferences,
      @Nullable List<ScheduleDetail> schedules) {
    this.applicationName = applicationName;
    this.applicationFileHash = applicationFileHash;
    this.appRequest = appRequest;
    this.preferences = preferences;
    this.schedules = schedules;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getApplicationFileHash() {
    return applicationFileHash;
  }

  public AppRequest<?> getAppRequest() {
    return appRequest;
  }

  @Nullable
  public PreferencesDetail getPreferences() {
    return preferences;
  }

  @Nullable
  public List<ScheduleDetail> getSchedules() {
    return schedules;
  }
}
