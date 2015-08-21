/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.Location;

import javax.annotation.Nullable;

/**
 * Represents information of an application that is undergoing deployment.
 */
public final class ApplicationDeployable {
  private final Id.Application id;
  private final ApplicationSpecification specification;
  private final ApplicationSpecification existingAppSpec;
  private final ApplicationDeployScope applicationDeployScope;
  private final Location location;

  public ApplicationDeployable(Id.Application id, ApplicationSpecification specification,
                               @Nullable ApplicationSpecification existingAppSpec,
                               ApplicationDeployScope applicationDeployScope,
                               Location location) {
    this.id = id;
    this.specification = specification;
    this.existingAppSpec = existingAppSpec;
    this.applicationDeployScope = applicationDeployScope;
    this.location = location;
  }

  public Id.Application getId() {
    return id;
  }

  public ApplicationSpecification getSpecification() {
    return specification;
  }

  @Nullable
  public ApplicationSpecification getExistingAppSpec() {
    return existingAppSpec;
  }

  public ApplicationDeployScope getApplicationDeployScope() {
    return applicationDeployScope;
  }

  public Location getLocation() {
    return location;
  }
}
