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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

/**
 * POJO containing a {@link StreamSizeSchedule} and its state.
 */
public class StreamSizeScheduleState {
  private final Id.Program programId;
  private final SchedulableProgramType programType;
  private final StreamSizeSchedule streamSizeSchedule;
  private final long baseRunSize;
  private final long baseRunTs;
  private final long lastRunSize;
  private final long lastRunTs;
  private final boolean running;

  @VisibleForTesting
  public StreamSizeScheduleState(Id.Program programId, SchedulableProgramType programType,
                                 StreamSizeSchedule streamSizeSchedule,
                                 long baseRunSize, long baseRunTs, long lastRunSize, long lastRunTs, boolean running) {
    this.programId = programId;
    this.programType = programType;
    this.streamSizeSchedule = streamSizeSchedule;
    this.baseRunSize = baseRunSize;
    this.baseRunTs = baseRunTs;
    this.lastRunSize = lastRunSize;
    this.lastRunTs = lastRunTs;
    this.running = running;
  }

  public Id.Program getProgramId() {
    return programId;
  }

  public SchedulableProgramType getProgramType() {
    return programType;
  }

  public StreamSizeSchedule getStreamSizeSchedule() {
    return streamSizeSchedule;
  }

  public long getBaseRunSize() {
    return baseRunSize;
  }

  public long getBaseRunTs() {
    return baseRunTs;
  }

  public long getLastRunSize() {
    return lastRunSize;
  }

  public long getLastRunTs() {
    return lastRunTs;
  }

  public boolean isRunning() {
    return running;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("programId", programId)
      .add("programType", programType)
      .add("streamSizeSchedule", streamSizeSchedule)
      .add("baseRunSize", baseRunSize)
      .add("baseRunTs", baseRunTs)
      .add("lastRunSize", lastRunSize)
      .add("lastRunTs", lastRunTs)
      .add("running", running)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StreamSizeScheduleState that = (StreamSizeScheduleState) o;
    return Objects.equal(programId, that.programId) &&
      Objects.equal(programType, that.programType) &&
      Objects.equal(streamSizeSchedule, that.streamSizeSchedule) &&
      Objects.equal(baseRunSize, that.baseRunSize) &&
      Objects.equal(baseRunTs, that.baseRunTs) &&
      Objects.equal(lastRunSize, that.lastRunSize) &&
      Objects.equal(lastRunTs, that.lastRunTs) &&
      Objects.equal(running, that.running);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(programId, programType, streamSizeSchedule, baseRunSize,
                            baseRunTs, lastRunSize, lastRunTs, running);
  }
}
