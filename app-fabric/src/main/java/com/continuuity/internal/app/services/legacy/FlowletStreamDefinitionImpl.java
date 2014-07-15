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
 * Implementation of FlowletStreamDefinition.
 *
 * A FlowletStreamDefinition specifies an endpoint of an edge or connection in
 * a flow.
 */
public class FlowletStreamDefinitionImpl implements FlowletStreamDefinition {
  private String flowlet;
  private String stream;

  /**
   * Empty constructor of this object.
   */
  public FlowletStreamDefinitionImpl() {
  }

  /**
   * Constructor of FlowStreamDefinition by specifying the a stream of a flowlet.
   * @param flowlet name of the flowlet
   * @param stream  name of the flowlet's stream
   */
  public FlowletStreamDefinitionImpl(String flowlet, String stream) {
    this.flowlet = flowlet;
    this.stream = stream;
  }

  /**
   * Constructor of FlowStreamDefinition by specifing a stream of the flow`.
   * @param stream  name of the stream
   */
  public FlowletStreamDefinitionImpl(String stream) {
    this.flowlet = null;
    this.stream = stream;
  }

  @Override
  public String getFlowlet() {
    return flowlet;
  }

  @Override
  public String getStream() {
    return stream;
  }

  @Override
  public boolean isFlowStream() {
    return this.flowlet == null;
  }

  @Override
  public boolean isFlowletStream() {
    return this.flowlet != null;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("flowlet", flowlet)
        .add("stream", stream)
        .toString();
  }
}
