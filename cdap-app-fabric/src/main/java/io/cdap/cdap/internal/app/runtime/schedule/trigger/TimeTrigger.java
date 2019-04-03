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
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProtoTrigger;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A Trigger that schedules a ProgramSchedule, based upon a particular cron expression.
 */
public class TimeTrigger extends ProtoTrigger.TimeTrigger implements SatisfiableTrigger {
  private static final Logger LOG =
    LoggerFactory.getLogger(co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger.class);
  private static final Gson GSON = new Gson();
  private static final java.lang.reflect.Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  public TimeTrigger(String cronExpression) {
    super(cronExpression);
    validate();
  }

  @Override
  public void validate() {
    Schedulers.validateCronExpression(cronExpression);
  }

  @Override
  public boolean isSatisfied(ProgramSchedule schedule, List<Notification> notifications) {
    for (Notification notification : notifications) {
      if (isSatisfied(schedule, notification)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Set<String> getTriggerKeys() {
    return ImmutableSet.of();
  }

  @Override
  public List<TriggerInfo> getTriggerInfos(TriggerInfoContext context) {
    for (Notification notification : context.getNotifications()) {
      if (!isSatisfied(context.getSchedule(), notification)) {
        continue;
      }

      Long logicalStartTime = getLogicalStartTime(notification);
      if (logicalStartTime == null) {
        LOG.warn("The notification '{}' in the job of schedule '{}' does not contain logical start time",
                 notification, context.getSchedule());
        continue;
      }

      TriggerInfo triggerInfo = new DefaultTimeTriggerInfo(getCronExpression(), logicalStartTime);
      return Collections.singletonList(triggerInfo);
    }
    return Collections.emptyList();
  }

  @Override
  public void updateLaunchArguments(ProgramSchedule schedule, List<Notification> notifications,
                                    Map<String, String> systemArgs, Map<String, String> userArgs) {
    for (Notification notification : notifications) {
      if (!isSatisfied(schedule, notification)) {
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

  @Nullable
  private Long getLogicalStartTime(Notification notification) {
    String userOverridesJson = notification.getProperties().get(ProgramOptionConstants.USER_OVERRIDES);
    if (userOverridesJson == null) {
      return null;
    }
    Map<String, String> args = GSON.fromJson(userOverridesJson, STRING_STRING_MAP);
    String logicalStartTime = args.get(ProgramOptionConstants.LOGICAL_START_TIME);
    if (logicalStartTime == null) {
      return null;
    }
    try {
      return Long.valueOf(logicalStartTime);
    } catch (NumberFormatException e) {
      // This shouldn't happen
      LOG.warn("Unable to parse property '{}' as long from notification properties {}.",
               ProgramOptionConstants.LOGICAL_START_TIME, args);
      return null;
    }
  }

  /**
   * Checks if the given notification satisfies this trigger.
   */
  private boolean isSatisfied(ProgramSchedule schedule, Notification notification) {
    if (!notification.getNotificationType().equals(Notification.Type.TIME)) {
      return false;
    }

    String systemOverridesJson = notification.getProperties().get(ProgramOptionConstants.SYSTEM_OVERRIDES);
    if (systemOverridesJson == null) {
      return false;
    }

    Map<String, String> args = GSON.fromJson(systemOverridesJson, STRING_STRING_MAP);
    String cronExpr = args.get(ProgramOptionConstants.CRON_EXPRESSION);

    // See if the notification is from pre 4.3 system, which doesn't have the cron expression in the notification.
    // The checking is done by the fact that in pre 4.3 system, composite trigger is not supported,
    // hence if there is a time notification, it must be matching with this trigger.
    return getCronExpression().equals(cronExpr) || (cronExpr == null && schedule.getTrigger().getType() == Type.TIME);
  }
}
