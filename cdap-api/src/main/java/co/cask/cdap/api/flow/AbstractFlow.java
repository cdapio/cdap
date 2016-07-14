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

package co.cask.cdap.api.flow;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.FlowletConfigurer;
import co.cask.cdap.internal.api.AbstractProgramDatasetConfigurable;

/**
 * This abstract class provides a default implementation of {@link Flow} methods for easy extension.
 */
public class AbstractFlow extends AbstractProgramDatasetConfigurable<FlowConfigurer> implements Flow {
  private FlowConfigurer configurer;

  @Override
  public final void configure(FlowConfigurer configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * Configure the {@link Flow}.
   */
  protected void configure() {

  }

  /**
   * Returns the {@link FlowConfigurer} used for configuration. Only available during configuration time.
   *
   * @return {@link FlowConfigurer}
   */
  @Override
  protected final FlowConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Sets the name of the {@link Flow}.
   *
   * @param name name of the flow
   */
  protected void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Sets the description of the {@link Flow}.
   *
   * @param description description of the flow
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Adds a {@link Flowlet} to the {@link Flow}.
   *
   * @param flowlet {@link Flowlet}
   */
  protected void addFlowlet(Flowlet flowlet) {
    configurer.addFlowlet(flowlet);
  }

  /**
   * Add a flowlet to the flow with the minimum number of instances of the flowlet to start with.
   *
   * @param flowlet {@link Flowlet} instance to be added to the flow
   * @param instances number of instances of flowlet
   */
  protected void addFlowlet(Flowlet flowlet, int instances) {
    configurer.addFlowlet(flowlet, instances);
  }

  /**
   * Add a flowlet to flow with the specified name. The specified name overrides the one
   * in {@link FlowletConfigurer#setName}.
   *
   * @param name name of the flowlet
   * @param flowlet {@link Flowlet} instance to be added to the flow
   */
  protected void addFlowlet(String name, Flowlet flowlet) {
    configurer.addFlowlet(name, flowlet);
  }

  /**
   * Add a flowlet to the flow with the minimum number of instances of the flowlet to start with.
   * The specified name overrides the one in {@link FlowletConfigurer#setName}.
   *
   * @param name name of the flowlet
   * @param flowlet {@link Flowlet} instance to be added to the flow
   * @param instances number of instances for the flowlet
   */
  protected void addFlowlet(String name, Flowlet flowlet, int instances) {
    configurer.addFlowlet(name, flowlet, instances);
  }

  /**
   * Connect a source flowlet to a destination flowlet using their names.
   *
   * @param from name of the source {@link Flowlet}
   * @param to name of the destination {@link Flowlet}
   */
  protected void connect(String from, String to) {
    configurer.connect(from, to);
  }

  /**
   * Connect a {@link Flowlet} instance to another {@link Flowlet} instance.
   *
   * @param from source {@link Flowlet} instance
   * @param to destination {@link Flowlet} instance
   */
  protected void connect(Flowlet from, Flowlet to) {
    configurer.connect(from, to);
  }

  /**
   * Connect a {@link Flowlet} to another {@link Flowlet}
   *
   * @param from name of source {@link Flowlet}
   * @param to name of destination {@link Flowlet}
   */
  protected void connect(String from, Flowlet to) {
    configurer.connect(from, to);
  }

  /**
   * Connect a {@link Flowlet} instance to another {@link Flowlet}.
   *
   * @param from source {@link Flowlet} instance
   * @param to name of the destination {@link Flowlet}
   */
  protected void connect(Flowlet from, String to) {
    configurer.connect(from, to);
  }

  /**
   * Connect a {@link Stream} to a destination {@link Flowlet}
   *
   * @param stream name of the {@link Stream}
   * @param flowlet destination {@link Flowlet} instance
   */
  protected void connectStream(String stream, Flowlet flowlet) {
    configurer.connectStream(stream, flowlet);
  }

  /**
   * Connect a {@link Stream} to a destination {@link Flowlet}
   *
   * @param namespace namespace of the {@link Stream}
   * @param stream name of the {@link Stream}
   * @param flowlet destination {@link Flowlet} instance
   */
  protected void connectStream(String namespace, String stream, Flowlet flowlet) {
    configurer.connectStream(namespace, stream, flowlet);
  }

  /**
   * Connect a {@link Stream} to a destination {@link Flowlet}
   *
   * @param stream name of the {@link Stream}
   * @param flowlet name of the destination {@link Flowlet}
   */
  protected void connectStream(String stream, String flowlet) {
    configurer.connectStream(stream, flowlet);
  }

  /**
   * Connect a {@link Stream} to a destination {@link Flowlet}
   *
   * @param namespace namespace of the {@link Stream}
   * @param stream name of the {@link Stream}
   * @param flowlet name of the destination {@link Flowlet}
   */
  protected void connectStream(String namespace, String stream, String flowlet) {
    configurer.connectStream(namespace, stream, flowlet);
  }
}
