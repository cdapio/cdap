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

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ScheduleTaskRunner;
import co.cask.cdap.proto.Notification;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A trigger that must be satisfied before a schedule checks constraints.
 */
public interface SatisfiableTrigger extends Trigger {

  /**
   * Checks whether the given notifications can satisfy this trigger. Once the trigger is satisfied, it will
   * remain satisfied no matter what new notifications it receives.
   *
   *
   * @param schedule
   * @param notifications the notifications used to check whether this trigger is satisfied
   * @return {@code true} if this trigger is satisfied, {@code false} otherwise
   */
  boolean isSatisfied(ProgramSchedule schedule, List<Notification> notifications);

  /**
   * Get all trigger keys which will be used to index the schedule containing this trigger, so that we can
   * do reverse lookup to get the schedule when events relevant to the trigger are received.
   *
   * @return a set of trigger keys as {@link String}. The set will be never be null.
   */
  Set<String> getTriggerKeys();

  /**
   * Get the {@link TriggerInfo} constructed from this trigger with the given context.
   *
   * @param context the {@link TriggerInfoContext} that provides necessary information to build the {@link TriggerInfo}
   * @return An immutable list of {@link TriggerInfo}'s of this trigger. If the trigger is not
   *         composite trigger, the list only contains one trigger info for this trigger.
   *         If the trigger is a composite trigger, the list will contain all the satisfied non-composite triggers
   *         in the composite trigger.
   */
  List<TriggerInfo> getTriggerInfos(TriggerInfoContext context);

  /**
   * Called by the {@link ScheduleTaskRunner} before launching a given program. It gives triggers control on
   * what to pass to the user program at runtime.
   *
   * @param schedule the schedule that this trigger belongs to
   * @param notifications the list of notifications that have this trigger satisfied.
   * @param systemArgs system arguments to the runtime system
   * @param userArgs user arguments to the runtime system
   */
  void updateLaunchArguments(ProgramSchedule schedule, List<Notification> notifications,
                             Map<String, String> systemArgs, Map<String, String> userArgs);
}
