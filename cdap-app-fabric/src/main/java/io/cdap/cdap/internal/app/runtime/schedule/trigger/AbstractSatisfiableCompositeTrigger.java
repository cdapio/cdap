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
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Abstract base class for composite trigger.
 */
public abstract class AbstractSatisfiableCompositeTrigger
  extends ProtoTrigger.AbstractCompositeTrigger<SatisfiableTrigger> implements SatisfiableTrigger {
  // A map of non-composite trigger type and set of triggers of the same type
  private Map<Type, Set<SatisfiableTrigger>> unitTriggers;

  protected AbstractSatisfiableCompositeTrigger(Type type, List<SatisfiableTrigger> triggers) {
    super(type, triggers);
  }

  @Override
  public Set<String> getTriggerKeys() {
    // Only keep unique trigger keys in the set
    ImmutableSet.Builder<String> triggerKeysBuilder = ImmutableSet.builder();
    for (SatisfiableTrigger trigger : getTriggers()) {
      triggerKeysBuilder.addAll(trigger.getTriggerKeys());
    }
    return triggerKeysBuilder.build();
  }

  @Override
  public void updateLaunchArguments(ProgramSchedule schedule, List<Notification> notifications,
                                    Map<String, String> systemArgs, Map<String, String> userArgs) {
    for (SatisfiableTrigger trigger : getTriggers()) {
      trigger.updateLaunchArguments(schedule, notifications, systemArgs, userArgs);
    }
  }

  /**
   * Get all triggers which are not composite trigger in this trigger.
   */
  public Map<Type, Set<SatisfiableTrigger>> getUnitTriggers() {
    if (unitTriggers == null) {
      initializeUnitTriggers();
    }
    return unitTriggers;
  }

  private void initializeUnitTriggers() {
    unitTriggers = new HashMap<>();
    for (Trigger trigger : getTriggers()) {
      // Add current non-composite trigger to the corresponding set in the map
      Type triggerType = trigger.getType();
      if (trigger instanceof AbstractSatisfiableCompositeTrigger) {
        // If the current trigger is a composite trigger, add each of its unit triggers to the set according to type
        for (Map.Entry<Type, Set<SatisfiableTrigger>> entry :
          ((AbstractSatisfiableCompositeTrigger) trigger).getUnitTriggers().entrySet()) {
          Set<SatisfiableTrigger> innerUnitTriggerSet = unitTriggers.get(entry.getKey());
          if (innerUnitTriggerSet == null) {
            innerUnitTriggerSet = new HashSet<>();
            unitTriggers.put(entry.getKey(), innerUnitTriggerSet);
          }
          innerUnitTriggerSet.addAll(entry.getValue());
        }
      } else {
        // If the current trigger is a non-composite trigger, add it to the set according to its type
        Set<SatisfiableTrigger> triggerSet = unitTriggers.get(triggerType);
        if (triggerSet == null) {
          triggerSet = new HashSet<>();
          unitTriggers.put(triggerType, triggerSet);
        }
        triggerSet.add((SatisfiableTrigger) trigger);
      }
    }
  }

  /**
   * @return An immutable list of trigger info's of all the unit triggers in this composite trigger
   */
  public List<TriggerInfo> getUnitTriggerInfosAddRuntimeArgs(TriggerInfoContext context) {
    ImmutableList.Builder<TriggerInfo> unitTriggerInfos = ImmutableList.builder();
    for (Set<SatisfiableTrigger> triggeSet : getUnitTriggers().values()) {
      for (SatisfiableTrigger trigger : triggeSet) {
        unitTriggerInfos.addAll(trigger.getTriggerInfos(context));
      }
    }
    return unitTriggerInfos.build();
  }

  /**
   * Create a new trigger where all sub-triggers related to the given program have been removed.
   * Returns null if removing relevant sub-triggers results in a trigger that can never be satisfied.
   *
   * @param programId the program id of the deleted program
   * @return the new trigger, or {@code null} if result trigger will never be satisfied
   */
  @Nullable
  public abstract SatisfiableTrigger getTriggerWithDeletedProgram(ProgramId programId);
}
