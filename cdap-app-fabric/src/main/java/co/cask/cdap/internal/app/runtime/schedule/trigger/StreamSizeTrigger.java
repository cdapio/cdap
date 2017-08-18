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

import co.cask.cdap.api.schedule.StreamSizeTriggerInfo;
import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Trigger that schedules a ProgramSchedule, based on new data in a stream.
 */
public class StreamSizeTrigger extends ProtoTrigger.StreamSizeTrigger implements SatisfiableTrigger {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSizeTrigger.class);
  private static final Gson GSON = new Gson();
  private static final java.lang.reflect.Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

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

  @Override
  public List<TriggerInfo> getTriggerInfosAddArgumentOverrides(TriggerInfoContext context, Map<String, String> sysArgs,
                                                               Map<String, String> userArgs) {
    Notification notification = context.getNotifications().get(0);
    String userOverridesString = notification.getProperties().get(ProgramOptionConstants.USER_OVERRIDES);
    if (userOverridesString == null) {
        LOG.warn("The notification '{}' in the job of schedule '{}' does not contain property '{}'.",
                 notification, context.getSchedule(), ProgramOptionConstants.USER_OVERRIDES);
        return ImmutableList.of();
    }
    Map<String, String> userOverrides = GSON.fromJson(userOverridesString, STRING_STRING_MAP);
    try {
      long logicalStartTime = Long.valueOf(userOverrides.get(ProgramOptionConstants.LOGICAL_START_TIME));
      long streamSize = Long.valueOf(userOverrides.get(ProgramOptionConstants.RUN_DATA_SIZE));
      long basePollingTime = Long.valueOf(userOverrides.get(ProgramOptionConstants.RUN_BASE_COUNT_TIME));
      long baseStreamSize = Long.valueOf(userOverrides.get(ProgramOptionConstants.RUN_BASE_COUNT_SIZE));
      TriggerInfo triggerInfo =
        new StreamSizeTriggerInfo(streamId.getNamespace(), streamId.getStream(), triggerMB,
                                  logicalStartTime, streamSize, basePollingTime, baseStreamSize);
      return ImmutableList.of(triggerInfo);
    } catch (NumberFormatException e) {
      LOG.warn("Failed to parse long value from notification '{}'", notification, e);
    }
    return ImmutableList.of();
  }
}
