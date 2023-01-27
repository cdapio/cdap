/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.sourcecontrol;

import javax.annotation.Nullable;

/**
 * The request class to set or validate repository configuration.
 */
public class RepositoryConfigRequest extends RepositoryConfig {
  private final boolean validate;

  public RepositoryConfigRequest(Provider provider, String link, String defaultBranch, AuthConfig authConfig,
                                 @Nullable String pathPrefix, boolean validate) {
    super(provider, link, defaultBranch, authConfig, pathPrefix);
    this.validate = validate;
  }

  public boolean shouldValidate() {
    return validate;
  }
}
