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

import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Trigger that schedules a ProgramSchedule, based on new data in a stream.
 */
public class StreamSizeTrigger extends ProtoTrigger.StreamSizeTrigger implements SatisfiableTrigger {
  private static final Logger LOG =
    LoggerFactory.getLogger(co.cask.cdap.internal.app.runtime.schedule.trigger.StreamSizeTrigger.class);
  private static final Gson GSON = new Gson();
  private static final java.lang.reflect.Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  public StreamSizeTrigger(StreamId streamId, int triggerMB) {
    super(streamId, triggerMB);
  }

  @Override
  public boolean isSatisfied(ProgramSchedule schedule, List<Notification> notifications) {
    return true;
  }

  @Override
  public Set<String> getTriggerKeys() {
    return ImmutableSet.of();
  }

  @Override
  public List<TriggerInfo> getTriggerInfos(TriggerInfoContext context) {
    for (Notification notification : context.getNotifications()) {
      if (notification.getNotificationType() != Notification.Type.STREAM_SIZE) {
        continue;
      }

      String systemOverridesJson = notification.getProperties().get(ProgramOptionConstants.SYSTEM_OVERRIDES);
      if (systemOverridesJson == null) {
        LOG.warn("The notification '{}' in the job of schedule '{}' does not contain property '{}'.",
                 notification, context.getSchedule(), ProgramOptionConstants.SYSTEM_OVERRIDES);
        continue;
      }

      Map<String, String> systemOverrides = GSON.fromJson(systemOverridesJson, STRING_STRING_MAP);
      try {
        long streamSize = Long.valueOf(systemOverrides.get(ProgramOptionConstants.RUN_DATA_SIZE));
        long basePollingTime = Long.valueOf(systemOverrides.get(ProgramOptionConstants.RUN_BASE_COUNT_TIME));
        long baseStreamSize = Long.valueOf(systemOverrides.get(ProgramOptionConstants.RUN_BASE_COUNT_SIZE));
        TriggerInfo triggerInfo = new DefaultStreamSizeTriggerInfo(streamId.getNamespace(), streamId.getStream(),
                                                                   triggerMB, streamSize,
                                                                   basePollingTime, baseStreamSize);
        return Collections.singletonList(triggerInfo);
      } catch (NumberFormatException e) {
        LOG.warn("Failed to parse long value from notification '{}'", notification, e);
      }
    }
    return Collections.emptyList();
  }

  @Override
  public void updateLaunchArguments(ProgramSchedule schedule, List<Notification> notifications,
                                    Map<String, String> systemArgs, Map<String, String> userArgs) {
    for (Notification notification : notifications) {
      if (notification.getNotificationType() != Notification.Type.STREAM_SIZE) {
        continue;
      }

      String systemOverridesJson = notification.getProperties().get(ProgramOptionConstants.SYSTEM_OVERRIDES);
      String userOverridesJson = notification.getProperties().get(ProgramOptionConstants.USER_OVERRIDES);
      if (userOverridesJson == null || systemOverridesJson == null) {
        // Ignore the malformed notification
        continue;
      }

      systemArgs.putAll(GSON.<Map<String, String>>fromJson(systemOverridesJson, STRING_STRING_MAP));
      userArgs.putAll(GSON.<Map<String, String>>fromJson(userOverridesJson, STRING_STRING_MAP));
      return;
    }
  }
}
