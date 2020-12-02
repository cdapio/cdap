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
 * Represents a capability operation record in store
 */
public class CapabilityOperationRecord {

  /**
   * Capability name
   */
  private final String capability;

  /**
   * Current action being applied for the capability
   */
  private final CapabilityAction actionType;

  /**
   * Configuration for the capability
   */
  private final CapabilityConfig capabilityConfig;

  public CapabilityOperationRecord(String capability, CapabilityAction actionType,
                                   CapabilityConfig capabilityConfig) {
    this.capability = capability;
    this.actionType = actionType;
    this.capabilityConfig = capabilityConfig;
  }

  /**
   * @return capability
   */
  public String getCapability() {
    return capability;
  }

  /**
   * @return action
   */
  public CapabilityAction getActionType() {
    return actionType;
  }

  /**
   * @return config
   */
  public CapabilityConfig getCapabilityConfig() {
    return capabilityConfig;
  }
}
