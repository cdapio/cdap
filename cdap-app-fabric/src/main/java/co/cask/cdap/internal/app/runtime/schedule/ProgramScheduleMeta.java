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

package co.cask.cdap.internal.app.runtime.schedule;

/**
 * Meta data about a program schedule, including its current status and last-updated timestamp.
 */
public class ProgramScheduleMeta {

  private final ProgramScheduleStatus status;
  private final long lastUpdated;

  public ProgramScheduleMeta(ProgramScheduleStatus status, long lastUpdated) {
    this.status = status;
    this.lastUpdated = lastUpdated;
  }

  public ProgramScheduleStatus getStatus() {
    return status;
  }

  public long getLastUpdated() {
    return lastUpdated;
  }
}
