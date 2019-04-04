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

import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.runtime.spi.profile.ProfileStatus;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Information of a profile. It encapsulates any information required to setup and teardown the program execution
 * environment. A profile is identified by name and must be assigned a provisioner and its related configuration.
 */
public class Profile {
  public static final String NATIVE_NAME = "native";
  public static final Profile NATIVE = new Profile(NATIVE_NAME, NATIVE_NAME, "Runs programs locally on the cluster",
                                                   EntityScope.SYSTEM,
                                                   new ProvisionerInfo(NATIVE_NAME, Collections.emptyList()));
  private final String name;
  private final String label;
  private final String description;
  private final EntityScope scope;
  private final ProfileStatus status;
  private final ProvisionerInfo provisioner;
  // this timestamp has time unit seconds
  private final long created;

  public Profile(String name, String label, String description, ProvisionerInfo provisioner) {
    this(name, label, description, EntityScope.USER, provisioner);
  }

  public Profile(String name, String label, String description, EntityScope scope,
                 ProvisionerInfo provisioner) {
    this(name, label, description, scope, ProfileStatus.ENABLED, provisioner);
  }

  public Profile(String name, String label, String description, EntityScope scope, ProfileStatus status,
                 ProvisionerInfo provisioner) {
    this(name, label, description, scope, status, provisioner,
         TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
  }

  public Profile(String name, String label, String description, EntityScope scope, ProfileStatus status,
                 ProvisionerInfo provisioner, long created) {
    this.name = name;
    this.label = label;
    this.description = description;
    this.scope = scope;
    this.status = status;
    this.provisioner = provisioner;
    this.created = created;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getLabel() {
    return label;
  }

  public String getScopedName() {
    return scope.name() + ":" + name;
  }

  public EntityScope getScope() {
    return scope;
  }

  public ProvisionerInfo getProvisioner() {
    return provisioner;
  }

  public ProfileStatus getStatus() {
    return status;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  public long getCreatedTsSeconds() {
    return created;
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
      Objects.equals(label, profile.label) &&
      Objects.equals(description, profile.description) &&
      Objects.equals(scope, profile.scope) &&
      Objects.equals(status, profile.status) &&
      Objects.equals(provisioner, profile.provisioner);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, label, description, scope, provisioner);
  }

  @Override
  public String toString() {
    return "Profile{" +
      "name='" + name + '\'' +
      ", label='" + label + '\'' +
      ", description='" + description + '\'' +
      ", scope=" + scope +
      ", status=" + status +
      ", provisioner=" + provisioner +
      ", creationTimeSeconds=" + created +
      '}';
  }
}
