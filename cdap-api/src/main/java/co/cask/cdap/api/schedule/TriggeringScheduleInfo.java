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

import java.util.List;
import java.util.Map;

/**
 * The information of a schedule that can be used by the program launched by the schedule.
 */
public interface TriggeringScheduleInfo {
  /**
   * @return Schedule's name, which is unique in an application.
   */
  String getName();

  /**
   * @return Description of the schedule.
   */
  String getDescription();

  /**
   * @return An immutable list of trigger information contained in this schedule. If the trigger is not
   *         composite trigger, the list only contains one trigger info for this trigger.
   *         If the trigger is a composite trigger, the list will contain all the non-composite triggers
   *         in the composite trigger.
   */
  List<TriggerInfo> getTriggerInfos();

  /**
   * @return An immutable map containing the properties of the schedule.
   */
  Map<String, String> getProperties();
}
