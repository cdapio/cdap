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

import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * A Trigger that schedules a ProgramSchedule, based on new data in a stream.
 */
public class StreamSizeTrigger extends ProtoTrigger.StreamSizeTrigger implements SatisfiableTrigger {

  public StreamSizeTrigger(StreamId streamId, int triggerMB) {
    super(streamId, triggerMB);
  }

  @Override
  public boolean isSatisfied(List<Notification> notifications) {
    return true;
  }

  @Override
  public Set<String> getTriggerKeys() {
    return ImmutableSet.of();
  }
}
