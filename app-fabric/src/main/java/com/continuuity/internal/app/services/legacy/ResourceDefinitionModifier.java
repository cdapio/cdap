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
 * Modifier used for making modification to a resource specification of a flowlet.
 */
public interface ResourceDefinitionModifier {
  /**
   * Sets a new CPU count for a flowlet.
   *
   * @param newCPU count for a flowlet.
   * @return old CPU count.
   */
  public int setCPU(int newCPU);

  /**
   * Sets a new Memory allocation for a flowlet.
   *
   * @param newMemory for a flowlet.
   * @return previous memory allocated.
   */
  public int setMemory(int newMemory);

  /**
   * Sets a new uplink bandwidth specification for a flowlet.
   *
   * @param newUplink bandwidth to be allocated.
   * @return old allocation.
   */
  public int setUplink(int newUplink);

  /**
   * Sets a new downlink bandwidth specification for a flowlet.
   *
   * @param newDownlink bandwidth to be allocated
   * @return old allocation.
   */
  public int setDownlink(int newDownlink);
}
