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

package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ProgramReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A Trigger that schedules a ProgramSchedule, when at least one of the internal triggers are satisfied.
 */
public class OrTrigger extends AbstractSatisfiableCompositeTrigger {

  public OrTrigger(SatisfiableTrigger... triggers) {
    this(Arrays.asList(triggers));
  }

  public OrTrigger(List<SatisfiableTrigger> triggers) {
    super(Type.OR, triggers);
  }

  @Override
  public boolean isSatisfied(ProgramSchedule schedule, List<Notification> notifications) {
    for (SatisfiableTrigger trigger : getTriggers()) {
      if (trigger.isSatisfied(schedule, notifications)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<TriggerInfo> getTriggerInfos(TriggerInfoContext context) {
    return getUnitTriggerInfosAddRuntimeArgs(context);
  }

  @Nullable
  @Override
  public SatisfiableTrigger getTriggerWithDeletedProgram(ProgramReference programReference) {
    List<SatisfiableTrigger> updatedTriggers = new ArrayList<>();
    for (SatisfiableTrigger trigger : getTriggers()) {
      if (trigger instanceof ProgramStatusTrigger &&
        programReference.equals(((ProgramStatusTrigger) trigger).getProgramReference())) {
        // this program status trigger will never be satisfied, skip adding it to updatedTriggers
        continue;
      }
      if (trigger instanceof AbstractSatisfiableCompositeTrigger) {
        SatisfiableTrigger updatedTrigger =
          ((AbstractSatisfiableCompositeTrigger) trigger).getTriggerWithDeletedProgram(programReference);
        if (updatedTrigger != null) {
          // add the updated composite trigger into updatedTriggers
          updatedTriggers.add(updatedTrigger);
        }
      } else {
        // the trigger is not a composite trigger, add it to updatedTriggers directly
        updatedTriggers.add(trigger);
      }
    }
    // if the updatedTriggers is empty, the OR trigger will never be satisfied
    if (updatedTriggers.isEmpty()) {
      return null;
    }
    // No need to wrap the only one remaining trigger with an OrTrigger
    if (updatedTriggers.size() == 1) {
      return updatedTriggers.get(0);
    }
    // return a new OR trigger constructed from the updated triggers
    return new io.cdap.cdap.internal.app.runtime.schedule.trigger.OrTrigger(updatedTriggers);
  }
}
