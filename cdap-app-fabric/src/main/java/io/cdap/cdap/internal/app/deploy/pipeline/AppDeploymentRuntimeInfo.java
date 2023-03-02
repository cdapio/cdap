/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy.pipeline;

import io.cdap.cdap.api.app.ApplicationSpecification;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * App deployment runtime information, including program options and existing app spec, existing app
 * spec will be null if this is a preview run
 */
public class AppDeploymentRuntimeInfo {

  @Deprecated
  private final ApplicationSpecification existingAppSpec;
  private final Map<String, String> userArguments;
  private final Map<String, String> systemArguments;

  public AppDeploymentRuntimeInfo(@Nullable ApplicationSpecification existingAppSpec,
      Map<String, String> userArguments,
      Map<String, String> systemArguments) {
    this.existingAppSpec = existingAppSpec;
    this.userArguments = userArguments;
    this.systemArguments = systemArguments;
  }

  @Deprecated
  @Nullable
  public ApplicationSpecification getExistingAppSpec() {
    return existingAppSpec;
  }

  public Map<String, String> getUserArguments() {
    return userArguments;
  }

  public Map<String, String> getSystemArguments() {
    return systemArguments;
  }
}
