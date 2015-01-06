/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.api.workflow;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.common.PropertyProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Specification for a {@link Workflow}
 *
 * <p>
 * Example WorkflowSpecification for a scheduled workflow:
 *
 *  <pre>
 *    <code>
 *      {@literal @}Override
 *      public void configure() {
 *        setName("PurchaseHistoryWorkflow");
 *        setDescription("PurchaseHistoryWorkflow description");
 *        addMapReduce("PurchaseHistoryBuilder");
 *        addSchedule("DailySchedule");
 *      }
 *    </code>
 *  </pre>
 *
 * See the Purchase example application.
 */
public final class WorkflowSpecification implements ProgramSpecification, PropertyProvider {
  private final String className;
  private final String name;
  private final String description;
  private final Map<String, WorkflowActionSpecification> customActionMap;
  private final List<WorkflowActionEntry> actions;
  private final List<String> schedules;
  private final Map<String, String> properties;

  public WorkflowSpecification(String className, String name, String description,
                                      Map<String, String> properties,
                                      List<WorkflowActionEntry> actions,
                                      Map<String, WorkflowActionSpecification> customActionMap,
                                      List<String> schedules) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = properties == null ? Collections.unmodifiableMap(new HashMap<String, String>()) :
                                           Collections.unmodifiableMap(new HashMap<String, String>(properties));
    this.actions = Collections.unmodifiableList(new ArrayList<WorkflowActionEntry>(actions));
    this.schedules = Collections.unmodifiableList(new ArrayList<String>(schedules));
    this.customActionMap = Collections.unmodifiableMap(new HashMap(customActionMap));
  }

  @Override
  public String getClassName() {
    return className;
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
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  public List<WorkflowActionEntry> getActions() {
    return actions;
  }

  public List<String> getSchedules() {
    return schedules;
  }

  public Map<String, WorkflowActionSpecification> getCustomActionMap() {
    return customActionMap;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WorkflowSpecification{");
    sb.append("className='").append(className).append('\'');
    sb.append(", name='").append(name).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", customActionMap=").append(customActionMap);
    sb.append(", actions=").append(actions);
    sb.append(", schedules=").append(schedules);
    sb.append(", properties=").append(properties);
    sb.append('}');
    return sb.toString();
  }
}
