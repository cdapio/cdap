/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

/**
 * Represents a capability record in store
 */
public class CapabilityStatusRecord {

  /**
   * Capability name
   */
  private final String capability;

  /**
   * Current status of the capability
   */
  private final CapabilityStatus capabilityStatus;

  /**
   * Current configuration of the capability
   */
  private final CapabilityConfig capabilityConfig;

  public CapabilityStatusRecord(String capability, CapabilityStatus capabilityStatus,
                                CapabilityConfig capabilityConfig) {
    this.capability = capability;
    this.capabilityStatus = capabilityStatus;
    this.capabilityConfig = capabilityConfig;
  }

  /**
   * @return capability
   */
  public String getCapability() {
    return capability;
  }

  /**
   * @return status
   */
  public CapabilityStatus getCapabilityStatus() {
    return capabilityStatus;
  }

  /**
   * @return configuration
   */
  public CapabilityConfig getCapabilityConfig() {
    return capabilityConfig;
  }
}
