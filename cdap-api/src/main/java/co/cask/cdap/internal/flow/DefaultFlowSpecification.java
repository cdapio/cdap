/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.flow;

import co.cask.cdap.api.AbstractProgramSpecification;
import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ProgramSpecification} for {@link Flow}.
 */
public final class DefaultFlowSpecification extends AbstractProgramSpecification implements FlowSpecification {
  private final Map<String, FlowletDefinition> flowlets;
  private final List<FlowletConnection> connections;

  public DefaultFlowSpecification(String name, String description,
                                  Map<String, FlowletDefinition> flowlets, List<FlowletConnection> connections) {
    this(null, name, description, flowlets, connections);
  }

  public DefaultFlowSpecification(String className, FlowSpecification other) {
    this(className, other.getName(), other.getDescription(), other.getFlowlets(), other.getConnections());
  }

  public DefaultFlowSpecification(String className, String name, String description,
                                  Map<String, FlowletDefinition> flowlets, List<FlowletConnection> connections) {
    // Flows as of now don't support plugin and is deprecated hence just pass empty map as plugins
    super(className, name, description, Collections.emptyMap());
    this.flowlets = Collections.unmodifiableMap(new HashMap<>(flowlets));
    this.connections = Collections.unmodifiableList(new ArrayList<>(connections));
  }

  @Override
  public Map<String, FlowletDefinition> getFlowlets() {
    return flowlets;
  }

  @Override
  public List<FlowletConnection> getConnections() {
    return connections;
  }
}
