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

import com.continuuity.api.flow.flowlet.Flowlet;

/**
 * FlowletDefinition provides a read-only interface for reading attributes of
 * a single flowlet.
 */
public interface FlowletDefinition {
  /**
   * Returns the name of a flowlet.
   *
   * @return name of flowlet.
   */
  public String getName();

  /**
   * Returns the class associated with the flowlet.
   * This information is provided during programmtic configuration of a Flow.
   *
   * @return class associated with flowlet.
   */
  public Class<? extends Flowlet> getClazz();

  /**
   * Name of the class associated with flowlet.
   * This information is provided when flow is configured through flows.json.
   *
   * @return name of the class associated with flowlet.
   */
  public String getClassName();

  /**
   * Returns number of instances of a flowlet in a flow.
   *
   * @return number of instances of a flowlet.
   */
  public int getInstances();

  /**
   * Returns the resource specification associated with a flowlet.
   *
   * @return resource specification of a flowlet.
   */
  public ResourceDefinition getResources();


  /**
   * Returns the group Id associated with the flowlet.
   *
   * @return group Id associated with flowlet.
   */
  public long getGroupId();

}
