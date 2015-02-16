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
import co.cask.cdap.api.schedule.SchedulableProgramType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Specification for a {@link Workflow}
 */
public final class WorkflowSpecification implements ProgramSpecification, PropertyProvider {
  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;

  private final List<WorkflowNode> nodes;
  private final Map<String, WorkflowFork> forks;
  private final Set<String> mapreduces;
  private final Set<String> sparks;
  private final Map<String, WorkflowActionSpecification> customActionMap;

  public WorkflowSpecification(String className, String name, String description,
                                      Map<String, String> properties, List<WorkflowNode> nodes,
                                      Map<String, WorkflowFork> forks, Set<String> mapreduces, Set<String> sparks,
                                      Map<String, WorkflowActionSpecification> customActionMap) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = properties == null ? Collections.<String, String>emptyMap() :
                                           Collections.unmodifiableMap(new HashMap<String, String>(properties));
    this.nodes = Collections.unmodifiableList(new ArrayList<WorkflowNode>(nodes));
    this.forks = Collections.unmodifiableMap(new HashMap<String, WorkflowFork>(forks));
    this.mapreduces = Collections.unmodifiableSet(new HashSet<String>(mapreduces));
    this.sparks = Collections.unmodifiableSet(new HashSet<String>(sparks));
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

  public List<WorkflowNode> getNodes() {
    return nodes;
  }

  public Map<String, WorkflowFork> getForks() {
    return forks;
  }

  public Set<String> getMapreduces() {
    return mapreduces;
  }

  public Set<String> getSparks() {
    return sparks;
  }

  public Map<String, WorkflowActionSpecification> getCustomActionMap() {
    return customActionMap;
  }

  public ScheduleProgramInfo getProgramInfo(String name) {
    if (mapreduces.contains(name)) {
      return new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, name);
    } else if (sparks.contains(name)) {
      return new ScheduleProgramInfo(SchedulableProgramType.SPARK, name);
    } else if (customActionMap.containsKey(name)) {
      return new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION, name);
    }
    throw new RuntimeException(String.format("Program with name '%s' not found in the Workflow '%s'", name, getName()));
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
    sb.append(", mapreduces=").append(mapreduces);
    sb.append(", sparks=").append(sparks);
    sb.append(", customActionMap=").append(customActionMap);
    sb.append('}');
    return sb.toString();
  }
}
