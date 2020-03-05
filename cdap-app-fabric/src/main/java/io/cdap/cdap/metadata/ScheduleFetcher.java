/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleNotFoundException;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;

import java.io.IOException;
import java.util.List;

/**
 * Interface for fetching schedule(s)
 */
public interface ScheduleFetcher {
  /**
   * Get schedule details for the given {@code scheduleId}
   * @param scheduleId the id of the schedule to fetch {@code ScheduleDetail} for
   * @return the schedule detail
   * @throws IOException if failed to get the detail of the given schedule
   * @throws ScheduleNotFoundException if the given schedule doesn't exist.
   */
  ScheduleDetail get(ScheduleId scheduleId) throws IOException, ScheduleNotFoundException;

  /**
   *
   */
  /**
   * Get the list of schedules for the given program id
   * @param programId the id of the program to get the list of schedules for
   * @return a list of schedules set on the program
   * @throws IOException if failed to get the list of schedules for the given program
   * @throws ProgramNotFoundException if the given program id doesn't exist
   */
  List<ScheduleDetail> list(ProgramId programId) throws IOException, ProgramNotFoundException;
}
