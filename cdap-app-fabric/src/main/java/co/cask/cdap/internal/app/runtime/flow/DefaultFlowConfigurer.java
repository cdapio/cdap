/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowConfigurer;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.api.DefaultDatasetConfigurer;
import co.cask.cdap.internal.flow.DefaultFlowSpecification;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.OutputEmitterFieldExtractor;
import co.cask.cdap.internal.specification.ProcessMethodExtractor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link FlowConfigurer}.
 */
public class DefaultFlowConfigurer extends DefaultDatasetConfigurer implements FlowConfigurer {
  private String className;
  private String name;
  private String description;
  private Map<String, FlowletDefinition> flowlets;
  private List<FlowletConnection> connections;

  public DefaultFlowConfigurer(Flow flow) {
    this.className = flow.getClass().getName();
    this.name = flow.getClass().getSimpleName();
    this.description = "";
    this.flowlets = Maps.newHashMap();
    this.connections = Lists.newArrayList();
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void addFlowlet(Flowlet flowlet) {
    addFlowlet(flowlet, 1);
  }

  @Override
  public void addFlowlet(Flowlet flowlet, int instances) {
    addFlowlet(null, flowlet);
  }

  @Override
  public void addFlowlet(String name, Flowlet flowlet) {
    addFlowlet(name, flowlet, 1);
  }

  private String getFlowletName(Flowlet flowlet) {
    DefaultFlowletConfigurer flowletConfigurer = new DefaultFlowletConfigurer(flowlet);
    flowlet.configure(flowletConfigurer);
    FlowletSpecification flowletSpecification = flowletConfigurer.createSpecification();
    return flowletSpecification.getName();
  }

  @Override
  public void addFlowlet(String name, Flowlet flowlet, int instances) {
    Preconditions.checkNotNull(flowlet, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
    DefaultFlowletConfigurer flowletConfigurer = new DefaultFlowletConfigurer(flowlet);
    flowlet.configure(flowletConfigurer);
    FlowletSpecification flowletSpecification = flowletConfigurer.createSpecification();
    Map<String, Set<Type>> inputTypes = new HashMap<>();
    Map<String, Set<Type>> outputTypes = new HashMap<>();
    Reflections.visit(flowlet, flowlet.getClass(),
                      new OutputEmitterFieldExtractor(outputTypes),
                      new ProcessMethodExtractor(inputTypes));
    FlowletDefinition flowletDef = new FlowletDefinition(name, inputTypes, outputTypes,
                                                         flowletSpecification, instances);
    String flowletName = flowletDef.getFlowletSpec().getName();
    Preconditions.checkArgument(instances > 0, String.format(UserMessages.getMessage(UserErrors.INVALID_INSTANCES),
                                                             flowletName, instances));
    Preconditions.checkArgument(!flowlets.containsKey(flowletName),
                                UserMessages.getMessage(UserErrors.INVALID_FLOWLET_EXISTS), flowletName);
    flowlets.put(flowletName, flowletDef);
    addStreams(flowletConfigurer.getStreams());
    addDatasetSpecs(flowletConfigurer.getDatasetSpecs());
    addDatasetModules(flowletConfigurer.getDatasetModules());
  }

  @Override
  public void connect(Flowlet from, Flowlet to) {
    Preconditions.checkArgument(from != null && to != null, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
    connect(getFlowletName(from), getFlowletName(to));
  }

  @Override
  public void connect(String from, String to) {
    Preconditions.checkArgument(from != null && to != null, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
    Preconditions.checkArgument(flowlets.containsKey(from),
                                UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NAME), from);
    Preconditions.checkArgument(flowlets.containsKey(to),
                                UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NAME), to);
    connections.add(new FlowletConnection(FlowletConnection.Type.FLOWLET, from, to));
  }

  @Override
  public void connect(Flowlet from, String to) {
    Preconditions.checkNotNull(from, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
    connect(getFlowletName(from), to);
  }

  @Override
  public void connect(String from, Flowlet to) {
    Preconditions.checkNotNull(to, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
    connect(from, getFlowletName(to));
  }

  @Override
  public void connectStream(String stream, Flowlet flowlet) {
    connectStream(null, stream, flowlet);
  }

  @Override
  public void connectStream(@Nullable String namespace, String stream, Flowlet flowlet) {
    Preconditions.checkNotNull(flowlet, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
    connectStream(namespace, stream, getFlowletName(flowlet));
  }

  @Override
  public void connectStream(String stream, String flowlet) {
    connectStream(null, stream, flowlet);
  }

  @Override
  public void connectStream(@Nullable String namespace, String stream, String flowlet) {
    Preconditions.checkNotNull(stream, UserMessages.getMessage(UserErrors.INVALID_STREAM_NULL));
    Preconditions.checkNotNull(flowlet, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
    Preconditions.checkArgument(flowlets.containsKey(flowlet),
                                UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NAME), flowlet);
    connections.add(new FlowletConnection(FlowletConnection.Type.STREAM, namespace, stream, flowlet));
  }

  public FlowSpecification createSpecification() {
    return new DefaultFlowSpecification(className, name, description, flowlets, connections);
  }
}
