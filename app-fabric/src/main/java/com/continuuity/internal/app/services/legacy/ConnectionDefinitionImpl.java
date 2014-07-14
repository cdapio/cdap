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

import com.google.common.base.Objects;

/**
 * Default implementation of ConnectionDefinition interface.
 */
public class ConnectionDefinitionImpl implements ConnectionDefinition {
  /**
   * Endpoint specification of an edge starting from a flowlet.
   */
  private FlowletStreamDefinitionImpl from;

  /**
   * Endpoint specification of an edge ending at a flowlet.
   */
  private FlowletStreamDefinitionImpl to;


  /**
   * Empty construction of object.
   */
  public ConnectionDefinitionImpl() {

  }

  /**
   * Construct the ConnectionDefinition object by specifying the endpoints of the edge connecting two flowlets.
   * @param from endpoint
   * @param to   endpoint.
   */
  public ConnectionDefinitionImpl(FlowletStreamDefinitionImpl from, FlowletStreamDefinitionImpl to) {
    this.from = from;
    this.to = to;
  }

  /**
   * Returns the endpoint for an edge starting from a flowlet.
   *
   * @return definition of edge starting from a flowlet.
   */
  @Override
  public FlowletStreamDefinition getFrom() {
    return from;
  }

  /**
   * Sets the specification of endpoint starting from a flowlet.
   *
   * @param from endpoint specification.
   */
  public void setFrom(FlowletStreamDefinitionImpl from) {
    this.from = from;
  }

  /**
   * Returns the endpoint for an edge ending at a flowlet.
   *
   * @return definition of edge ending at a flowlet.
   */
  @Override
  public FlowletStreamDefinition getTo() {
    return to;
  }

  /**
   * Sets the specification of endpoint ending at a flowlet.
   *
   * @param to endpoint specification.
   */
  public void setTo(FlowletStreamDefinitionImpl to) {
    this.to = to;
  }
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("from", from)
        .add("to", to)
        .toString();
  }
}
