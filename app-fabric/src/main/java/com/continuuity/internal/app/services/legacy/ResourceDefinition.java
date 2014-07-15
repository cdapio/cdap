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
 * ResourceDefinition is a read-only interface for specifing the resources needed by a flowlet.
 */
public interface ResourceDefinition {
  /**
   * Returns number of CPUs to be allocated to flowlet.
   *
   * @return number of cpus
   */
  public int getCPU();

  /**
   * Returns amount of memory to be allocated to a flowlet in MB.
   * @return amount of memory in MB.
   */
  public int getMemoryInMB();

  /**
   * Returns uplink bandwidth specification in Mbps.
   *
   * @return uplink bandwidth in Mbps.
   */
  public int getUpLinkInMbps();

  /**
   * Returns downlink bandwidth specification in Mbps.
   *
   * @return downlink bandwidth in Mbps.
   */
  public int getDownLinkInMbps();
}
