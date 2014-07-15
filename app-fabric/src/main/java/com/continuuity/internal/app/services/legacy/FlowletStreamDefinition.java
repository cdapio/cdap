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

/**
 * Provides the definition for providing an endpoint of an edge connecting two flowlets,
 * or connecting a flowlet with a stream.
 *
 * If the flowlet name is set, then this connects to the stream of that flowlet with the given stream name
 * If the flowlet name is null, then this connects to named stream of the enclosing flow
 */
public interface FlowletStreamDefinition {
  /**
   * Returns the name of the flowlet, or null is the connection is to a flow stream
   * @return name of the flowlet.
   */
  public String getFlowlet();

  /**
   * Return the name of the stream.
   * @return the name of the stream.
   */
  public String getStream();

  /**
   * @return whether this connects to a stream of a flowlet
   */
  public boolean isFlowletStream();

  /**
   * @return whether this connects to a stream the enclosing flow
   */
  public boolean isFlowStream();
}
