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
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A Trigger that schedules a ProgramSchedule, when all internal triggers are satisfied.
 */
public class AndTrigger extends AbstractSatisfiableCompositeTrigger {

  public AndTrigger(SatisfiableTrigger... triggers) {
    this(Arrays.asList(triggers));
  }

  public AndTrigger(List<SatisfiableTrigger> triggers) {
    super(Type.AND, triggers);
  }


  @Override
  public boolean isSatisfied(ProgramSchedule schedule, List<Notification> notifications) {
    for (Trigger trigger : getTriggers()) {
      if (!((SatisfiableTrigger) trigger).isSatisfied(schedule, notifications)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<TriggerInfo> getTriggerInfos(TriggerInfoContext context) {
    return getUnitTriggerInfosAddRuntimeArgs(context);
  }

  @Nullable
  @Override
  public SatisfiableTrigger getTriggerWithDeletedProgram(ProgramId programId) {
    List<SatisfiableTrigger> updatedTriggers = new ArrayList<>();
    for (SatisfiableTrigger trigger : getTriggers()) {
      if (trigger instanceof ProgramStatusTrigger &&
        programId.equals(((ProgramStatusTrigger) trigger).getProgramId())) {
        // this program status trigger will never be satisfied, so the current AND trigger will never be satisfied
        return null;
      }
      if (trigger instanceof AbstractSatisfiableCompositeTrigger) {
        SatisfiableTrigger updatedTrigger =
          ((AbstractSatisfiableCompositeTrigger) trigger).getTriggerWithDeletedProgram(programId);
        if (updatedTrigger == null) {
          // the updated composite trigger will never be satisfied, so the AND trigger will never be satisfied
          return null;
        }
        // add the updated composite trigger into updatedTriggers
        updatedTriggers.add(updatedTrigger);
      } else {
        // the trigger is not a composite trigger, add it to updatedTriggers directly
        updatedTriggers.add(trigger);
      }
    }
    // No need to wrap the only one remaining trigger with an AndTrigger
    if (updatedTriggers.size() == 1) {
      return updatedTriggers.get(0);
    }
    // return a new AND trigger constructed from the updated triggers
    return new co.cask.cdap.internal.app.runtime.schedule.trigger.AndTrigger(updatedTriggers);
  }
}
