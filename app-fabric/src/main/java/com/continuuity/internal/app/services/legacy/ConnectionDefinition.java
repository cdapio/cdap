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
 * ConnectionDefinition provides information about a single edge in the DAG.
 * It provides specification about how the flowlets are connected together.
 */
public interface ConnectionDefinition {
  /**
   * Returns the endpoint for an edge starting from a flowlet.
   * @return definition of edge starting from a flowlet.
   */
  FlowletStreamDefinition getFrom();

  /**
   * Returns the endpoint for an edge ending at a flowlet.
   * @return definition of edge ending at a flowlet.
   */
  FlowletStreamDefinition getTo();
}
