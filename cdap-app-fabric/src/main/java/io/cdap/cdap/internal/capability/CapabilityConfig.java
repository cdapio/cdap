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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Configuration for applying an action for a capability
 */
public class CapabilityConfig {

  private final String label;
  private final CapabilityStatus status;
  private final String capability;
  private final List<SystemApplication> applications;
  private final List<SystemProgram> programs;

  public CapabilityConfig(String label, CapabilityStatus status, String capability,
                          Collection<? extends SystemApplication> applications,
                          Collection<? extends SystemProgram> programs) {
    this.label = label;
    this.status = status;
    this.capability = capability;
    this.applications = new ArrayList<>(applications);
    this.programs = new ArrayList<>(programs);
  }

  /**
   * @return label {@link String}
   */
  public String getLabel() {
    return label;
  }

  /**
   * @return {@link CapabilityAction}
   */
  public CapabilityStatus getStatus() {
    return status;
  }

  /**
   * @return {@link String} capability
   */
  public String getCapability() {
    return capability;
  }

  /**
   * @return {@link List} of {@link SystemApplication} for this capability. Could be null.
   */
  public List<SystemApplication> getApplications() {
    return applications;
  }

  /**
   * @return {@link List} of {@link SystemProgram} for this capability.
   */
  public List<SystemProgram> getPrograms() {
    return programs;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    CapabilityConfig otherConfig = (CapabilityConfig) other;
    return Objects.equals(label, otherConfig.label) &&
      status == otherConfig.status &&
      Objects.equals(capability, otherConfig.capability) &&
      Objects.equals(applications, otherConfig.applications) &&
      Objects.equals(programs, otherConfig.programs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, status, capability, applications, programs);
  }
}
