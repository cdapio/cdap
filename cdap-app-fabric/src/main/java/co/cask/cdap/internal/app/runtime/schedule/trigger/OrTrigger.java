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

import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.ProgramId;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A Trigger that schedules a ProgramSchedule, when at least one of the internal triggers are satisfied.
 */
public class OrTrigger extends AbstractCompositeTrigger implements SatisfiableTrigger {

  public OrTrigger(SatisfiableTrigger... triggers) {
    super(Type.OR, triggers);
  }

  @Override
  public boolean isSatisfied(ProgramSchedule schedule, List<Notification> notifications) {
    for (Trigger trigger : getTriggers()) {
      if (((SatisfiableTrigger) trigger).isSatisfied(schedule, notifications)) {
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
  public Trigger updateTriggerWithDeletedProgram(ProgramId programId) {
    List<SatisfiableTrigger> updatedTriggers = new ArrayList<>();
    for (Trigger trigger : getTriggers()) {
      if (trigger instanceof ProgramStatusTrigger &&
        programId.equals(((ProgramStatusTrigger) trigger).getProgramId())) {
        // this program status trigger will never be satisfied, skip adding it to updatedTriggers
        continue;
      }
      if (trigger instanceof co.cask.cdap.internal.app.runtime.schedule.trigger.AbstractCompositeTrigger) {
        Trigger updatedTrigger = ((co.cask.cdap.internal.app.runtime.schedule.trigger.AbstractCompositeTrigger) trigger)
          .updateTriggerWithDeletedProgram(programId);
        if (updatedTrigger == null) {
          // the updated composite trigger will never be satisfied, skip adding it to updatedTriggers
          continue;
        }
        // add the updated composite trigger into updatedTriggers
        updatedTriggers.add((SatisfiableTrigger) updatedTrigger);
      } else {
        // the trigger is not a composite trigger, add it to updatedTriggers directly
        updatedTriggers.add((SatisfiableTrigger) trigger);
      }
    }
    // if the updatedTriggers is empty, the OR trigger will never be satisfied
    if (updatedTriggers.isEmpty()) {
      return null;
    }
    // return a new OR trigger constructed from the updated triggers
    return new co.cask.cdap.internal.app.runtime.schedule.trigger.OrTrigger(
      updatedTriggers.toArray(new SatisfiableTrigger[updatedTriggers.size()]));
  }
}
