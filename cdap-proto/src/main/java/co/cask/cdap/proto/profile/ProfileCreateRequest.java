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

package co.cask.cdap.proto.profile;

import co.cask.cdap.proto.provisioner.ProvisionerInfo;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Information about a profile.
 */
public class ProfileCreateRequest {
  private final String description;
  private final ProvisionerInfo provisioner;

  public ProfileCreateRequest(String description, ProvisionerInfo provisioner) {
    this.description = description;
    this.provisioner = provisioner;
  }

  // this will only return null if there is no such field in json
  @Nullable
  public ProvisionerInfo getProvisioner() {
    // This is to make sure there is no null value in provisioner properties,
    // since Gson will deserialize non-existing property to null value.
    return provisioner == null ? null : new ProvisionerInfo(provisioner.getName(), provisioner.getProperties());
  }

  public String getDescription() {
    return description;
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
    return Objects.equals(description, that.description) &&
      Objects.equals(provisioner, that.provisioner);
  }

  @Override
  public int hashCode() {
    return Objects.hash(description, provisioner);
  }
}
