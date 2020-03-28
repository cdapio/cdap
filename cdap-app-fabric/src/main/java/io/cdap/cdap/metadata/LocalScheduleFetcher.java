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

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleNotFoundException;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.scheduler.ProgramScheduleService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Implementation for {@link ScheduleFetcher} that fetches {@link ScheduleDetail} directly from local store
 * via {@link ProgramScheduleService}
 */
public class LocalScheduleFetcher implements ScheduleFetcher {
  private final ProgramScheduleService programScheduleService;

  @Inject
  public LocalScheduleFetcher(ProgramScheduleService programScheduleService) {
    this.programScheduleService = programScheduleService;
  }

  @Override
  public ScheduleDetail get(ScheduleId scheduleId) throws IOException, ScheduleNotFoundException {
    ProgramScheduleRecord record =  null;
    try {
      record = programScheduleService.getRecord(scheduleId);
    } catch (NotFoundException e) {
      throw new ScheduleNotFoundException(scheduleId, e);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, ScheduleNotFoundException.class);
      throw new IOException(e);
    }
    return record.toScheduleDetail();
  }

    @Override
    public List<ScheduleDetail> list(ProgramId programId) throws IOException, ProgramNotFoundException {
      Predicate<ProgramScheduleRecord> predicate  = (record) -> true;
      Collection<ProgramScheduleRecord> schedules = null;
      try {
        schedules = programScheduleService.list(programId, predicate);
      } catch (Exception e) {
        Throwables.propagateIfPossible(e.getCause(), IOException.class, ProgramNotFoundException.class);
        throw new IOException(e);
      }
      return schedules.stream().map(ProgramScheduleRecord::toScheduleDetail).collect(Collectors.toList());
  }
}
