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

import co.cask.cdap.api.DatasetConfigurer;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.FlowletConfigurer;

/**
 * Configurer for configuring {@link Flow}.
 */
public interface FlowConfigurer extends DatasetConfigurer {

  /**
   * Set the name of the Flow.
   *
   * @param name name of the flow
   */
  void setName(String name);

  /**
   * Set the description of the Flow.
   *
   * @param description description of the flow
   */
  void setDescription(String description);

  /**
   * Add a Flowlet to the Flow.
   *
   * @param flowlet {@link Flowlet} instance to be added to the flow
   */
  void addFlowlet(Flowlet flowlet);

  /**
   * Add a flowlet to the flow with the minimum number of instances of the flowlet to start with.
   *
   * @param flowlet {@link Flowlet} instance to be added to the flow
   * @param instances number of instances for the flowlet
   */
  void addFlowlet(Flowlet flowlet, int instances);

  /**
   * Add a flowlet to flow with the specified name. The specified name overrides the one
   * in {@link FlowletConfigurer#setName}.
   *
   * @param name name of the flowlet
   * @param flowlet {@link Flowlet} instance to be added to the flow
   */
  void addFlowlet(String name, Flowlet flowlet);

  /**
   * Add a flowlet to the flow with the minimum number of instances of the flowlet to start with.
   * The specified name overrides the one in {@link FlowletConfigurer#setName}.
   *
   * @param name name of the flowlet
   * @param flowlet {@link Flowlet} instance to be added to the flow
   * @param instances number of instances for the flowlet
   */
  void addFlowlet(String name, Flowlet flowlet, int instances);

  /**
   * Connect a {@link Flowlet} instance to another {@link Flowlet} instance.
   *
   * @param from source {@link Flowlet} instance
   * @param to destination {@link Flowlet} instance
   */
  void connect(Flowlet from, Flowlet to);

  /**
   * Connect a source flowlet to a destination flowlet using their names.
   *
   * @param from name of the source {@link Flowlet}
   * @param to name of the destination {@link Flowlet}
   */
  void connect(String from, String to);

  /**
   * Connect a {@link Flowlet} instance to another {@link Flowlet}.
   *
   * @param from source {@link Flowlet} instance
   * @param to name of the destination {@link Flowlet}
   */
  void connect(Flowlet from, String to);

  /**
   * Connect a {@link Flowlet} to another {@link Flowlet}
   *
   * @param from name of source {@link Flowlet}
   * @param to name of destination {@link Flowlet}
   */
  void connect(String from, Flowlet to);

  /**
   * Connect a {@link Stream} to a destination {@link Flowlet}
   *
   * @param stream name of the {@link Stream}
   * @param flowlet destination {@link Flowlet} instance
   */
  void connectStream(String stream, Flowlet flowlet);

  /**
   * Connect a {@link Stream} to a destination {@link Flowlet}
   *
   * @param namespace of the {@link Stream}
   * @param stream name of the {@link Stream}
   * @param flowlet destination {@link Flowlet} instance
   */
  void connectStream(String namespace, String stream, Flowlet flowlet);

  /**
   * Connect a {@link Stream} to a destination {@link Flowlet}
   *
   * @param stream name of the {@link Stream}
   * @param flowlet name of the destination {@link Flowlet}
   */
  void connectStream(String stream, String flowlet);

  /**
   * Connect a {@link Stream} to a destination {@link Flowlet}
   *
   * @param namespace namespace of the {@link Stream}
   * @param stream name of the {@link Stream}
   * @param flowlet name of the destination {@link Flowlet}
   */
  void connectStream(String namespace, String stream, String flowlet);

}
