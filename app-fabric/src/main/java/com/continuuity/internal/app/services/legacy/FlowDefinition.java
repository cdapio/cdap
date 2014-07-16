/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.services.legacy;

import com.continuuity.common.utils.ImmutablePair;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * FlowDefinition is a read-only interface for reading the attributes of
 * a flow definition. This interface is generally passed around internally
 * in the system to ensure that the definition can not be mutated. In some
 * cases we would need to modify a flow and add more information, in that
 * case we use the FlowDefinitionModifier interface to do so.
 */
public interface FlowDefinition {
  /**
   * Returns the {@link MetaDefinition} section of a flow definition.
   *
   * @return meta section of the flow.
   */
  public MetaDefinition getMeta();

  /**
   * Returns a sets of data set names used by the flow
   *
   * @return set of data set names
   */
  public Set<String> getDatasets();

  /**
   * Returns collection of flow inputs from a flow definition
   *
   * @return collection of input streams.
   */
  public Collection<? extends FlowStreamDefinition> getFlowStreams();

  /**
   * Returns collection of flowlets from a flow definition.
   *
   * @return collection of flowlet.
   */
  public Collection<? extends FlowletDefinition> getFlowlets();

  /**
   * Returns a collection of connections connecting the flowlets.
   *
   * @return collection of connections.
   */
  public Collection<? extends ConnectionDefinition> getConnections();

  /**
   * List of streams and URI's associated with a flowlet.
   *
   *
   * @param flowlet Name of the flowlet
   * @return List of streams associated with the flowlet along with their URIs
   */
  public Map<String, ImmutablePair<URI, StreamType>> getFlowletStreams(String flowlet);
}
