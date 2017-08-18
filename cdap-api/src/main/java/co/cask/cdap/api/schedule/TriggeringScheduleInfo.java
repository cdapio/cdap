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

package co.cask.cdap.api.schedule;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The information of a schedule that can be used by the program launched by the schedule.
 */
public class TriggeringScheduleInfo implements Serializable {

  private final String name;
  private final String description;
  private final List<TriggerInfo> triggerInfos;
  private final Map<String, String> properties;

  public TriggeringScheduleInfo(String name, String description, List<TriggerInfo> triggerInfos,
                                Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.triggerInfos = Collections.unmodifiableList(new ArrayList<>(triggerInfos));
  }

  /**
   * @return Schedule's name, which is unique in an application.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Description of the schedule.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return An immutable list of trigger information contained in this schedule. If the trigger is not
   *         composite trigger, the list only contains one trigger info for this trigger.
   *         If the trigger is a composite trigger, the list will contain all the non-composite triggers
   *         in the composite trigger.
   */
  public List<TriggerInfo> getTriggerInfos() {
    return triggerInfos;
  }

  /**
   * @return An immutable map containing the properties of the schedule.
   */
  public Map<String, String> getProperties() {
    return properties;
  }
}
