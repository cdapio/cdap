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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Specification for a {@link Workflow}
 */
public final class WorkflowSpecification implements ProgramSpecification, PropertyProvider {
  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;

  private List<WorkflowNode> nodes = Lists.newArrayList();
  private Map<String, WorkflowForkSpecification> forks = Maps.newHashMap();
  private Map<String, ScheduleProgramInfo> actions = Maps.newHashMap();
  private Map<String, WorkflowActionSpecification> customActionMap = Maps.newHashMap();

  public WorkflowSpecification(String className, String name, String description,
                                      Map<String, String> properties, List<WorkflowNode> nodes,
                                      Map<String, WorkflowForkSpecification> forks,
                                      Map<String, ScheduleProgramInfo> actions,
                                      Map<String, WorkflowActionSpecification> customActionMap) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = properties == null ? Collections.<String, String>emptyMap() :
                                           Collections.unmodifiableMap(new HashMap<String, String>(properties));
    this.nodes = Collections.unmodifiableList(new ArrayList<WorkflowNode>(nodes));
    this.forks = Collections.unmodifiableMap(new HashMap<String, WorkflowForkSpecification>(forks));
    this.actions = Collections.unmodifiableMap(new HashMap<String, ScheduleProgramInfo>(actions));
    this.customActionMap = Collections.unmodifiableMap(
      new HashMap<String, WorkflowActionSpecification>(customActionMap));
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

  public List<WorkflowNode> getNodes() {
    return nodes;
  }

  public Map<String, WorkflowForkSpecification> getForks() {
    return forks;
  }

  public Map<String, WorkflowActionSpecification> getCustomActionMap() {
    return customActionMap;
  }

  public Map<String, ScheduleProgramInfo> getActions() {
    return actions;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("WorkflowSpecification{");
    sb.append("className='").append(className).append('\'');
    sb.append(", name='").append(name).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", properties=").append(properties);
    sb.append(", nodes=").append(nodes);
    sb.append(", forks=").append(forks);
    sb.append(", actions=").append(actions);
    sb.append(", customActionMap=").append(customActionMap);
    sb.append('}');
    return sb.toString();
  }
}
