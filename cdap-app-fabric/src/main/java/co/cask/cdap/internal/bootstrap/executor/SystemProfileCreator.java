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
 *
 */

package co.cask.cdap.internal.bootstrap.executor;

import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.profile.ProfileCreateRequest;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import com.google.inject.Inject;

/**
 * Creates a system profile if it doesn't already exist.
 */
public class SystemProfileCreator extends BaseStepExecutor<SystemProfileCreator.Arguments> {
  private final ProfileService profileService;

  @Inject
  SystemProfileCreator(ProfileService profileService) {
    this.profileService = profileService;
  }

  @Override
  public void execute(Arguments arguments) {
    Profile profile = new Profile(arguments.name, arguments.getLabel(), arguments.getDescription(), EntityScope.SYSTEM,
                                  arguments.getProvisioner());
    profileService.createIfNotExists(arguments.getId(), profile);
  }

  /**
   * Arguments required to create a profile
   */
  static class Arguments extends ProfileCreateRequest implements Validatable {
    private final String name;

    Arguments(String name, String label, String description, ProvisionerInfo provisioner) {
      super(label, description, provisioner);
      this.name = name;
    }

    private ProfileId getId() {
      return NamespaceId.SYSTEM.profile(name);
    }

    @Override
    public void validate() {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Profile name must be specified");
      }
      if (getProvisioner() == null) {
        throw new IllegalArgumentException("Profile provisioner must be specified.");
      }
      getId();
    }
  }
}
