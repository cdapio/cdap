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

import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;

import java.util.Collections;
import java.util.Objects;

/**
 * Information of a profile. It encapsulates any information required to setup and teardown the program execution
 * environment. A profile is identified by name and must be assigned a provisioner and its related configuration.
 */
public class Profile {
  public static final Profile DEFAULT = new Profile("default", "Runs programs locally on the cluster",
                                                    new ProvisionerInfo("yarn", Collections.emptyList()));
  private final String name;
  private final String description;
  private final EntityScope scope;
  private final ProvisionerInfo provisioner;

  public Profile(String name, String description, ProvisionerInfo provisioner) {
    this(name, description, EntityScope.USER, provisioner);
  }

  public Profile(String name, String description, EntityScope scope,
                 ProvisionerInfo provisioner) {
    this.name = name;
    this.description = description;
    this.scope = scope;
    this.provisioner = provisioner;
  }

  public String getName() {
    return name;
  }

  public EntityScope getScope() {
    return scope;
  }

  public ProvisionerInfo getProvisioner() {
    return provisioner;
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
    Profile profile = (Profile) o;
    return Objects.equals(name, profile.name) &&
      Objects.equals(description, profile.description) &&
      Objects.equals(scope, profile.scope) &&
      Objects.equals(provisioner, profile.provisioner);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, scope, provisioner);
  }
}
