/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common;

import co.cask.cdap.proto.Id;

/**
 * Thrown when the user tries to create a schedule that already exists.
 */
public class ScheduleAlreadyExistsException extends AlreadyExistsException {

  private final Id.Schedule schedule;

  public ScheduleAlreadyExistsException(Id.Schedule schedule) {
    super(schedule);
    this.schedule = schedule;
  }

  public Id.Schedule getSchedule() {
    return schedule;
  }
}
