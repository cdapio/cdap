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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.proto.id.StreamId;

/**
 * A Trigger that schedules a ProgramSchedule, based upon data availability in a stream.
 */
public class StreamSizeTrigger {
  private final StreamId streamName;
  private final int dataTriggerMB;

  public StreamSizeTrigger(StreamId streamName, int dataTriggerMB) {
    this.streamName = streamName;
    this.dataTriggerMB = dataTriggerMB;
  }

  public StreamId getStreamName() {
    return streamName;
  }

  public int getDataTriggerMB() {
    return dataTriggerMB;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamSizeTrigger that = (StreamSizeTrigger) o;
    return dataTriggerMB == that.dataTriggerMB && streamName.equals(that.streamName);
  }

  @Override
  public int hashCode() {
    int result = streamName.hashCode();
    result = 31 * result + dataTriggerMB;
    return result;
  }

  @Override
  public String toString() {
    return "StreamSizeTrigger{" +
      "streamName=" + streamName +
      ", dataTriggerMB=" + dataTriggerMB +
      '}';
  }
}
