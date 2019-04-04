/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.proto.profile;

import io.cdap.cdap.proto.provisioner.ProvisionerInfo;

import java.util.Objects;

/**
 * Information about a profile.
 */
public class ProfileCreateRequest {
  private final String label;
  private final String description;
  private final ProvisionerInfo provisioner;

  public ProfileCreateRequest(String label, String description, ProvisionerInfo provisioner) {
    this.label = label;
    this.description = description;
    this.provisioner = provisioner;
  }

  public ProvisionerInfo getProvisioner() {
    return provisioner;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Validate this is a valid object. Should be called when this is created through deserialization of user input.
   *
   * @throws IllegalArgumentException if the object is invalid
   */
  public void validate() {
    if (provisioner == null) {
      throw new IllegalArgumentException("Profile provisioner must be specified.");
    }
    provisioner.validate();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProfileCreateRequest that = (ProfileCreateRequest) o;
    return Objects.equals(label, that.label) &&
      Objects.equals(description, that.description) &&
      Objects.equals(provisioner, that.provisioner);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, description, provisioner);
  }
}
