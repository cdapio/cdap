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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link TriggeringScheduleInfo}.
 */
public class DefaultTriggeringScheduleInfo implements Serializable, TriggeringScheduleInfo {

  private final String name;
  private final String description;
  private final List<TriggerInfo> triggerInfos;
  private final Map<String, String> properties;

  public DefaultTriggeringScheduleInfo(String name, String description, List<TriggerInfo> triggerInfos,
                                       Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.triggerInfos = Collections.unmodifiableList(new ArrayList<>(triggerInfos));
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public List<TriggerInfo> getTriggerInfos() {
    return triggerInfos;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}
