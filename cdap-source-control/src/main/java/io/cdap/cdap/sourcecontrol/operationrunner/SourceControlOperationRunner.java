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

package io.cdap.cdap.sourcecontrol.operationrunner;

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;

/**
 * An interface encapsulating all operations needed for source control management
 */
public interface SourceControlOperationRunner {
  /**
   * @param pushAppContext {@link PushAppContext} pf the application to be pushed
   * @return {@link PushAppResponse} file-paths and file-hashes for the updated configs.
   * @throws PushFailureException when the push operation fails for any unexpected reason.
   * @throws NoChangesToPushException when the there's no change for the application to push.
   * @throws AuthenticationConfigException when the repository configuration is invalid.
   */
  PushAppResponse push(PushAppContext pushAppContext) throws PushFailureException, NoChangesToPushException,
    AuthenticationConfigException;

  /**
   * Gets an application spec from a Git repository.
   *
   * @param appRef The {@link ApplicationReference} of the application to pull from
   * @return the details of the pulled application.
   * @throws PullFailureException          when the operation fails for any reason.
   * @throws NotFoundException             when the requested application is not found in the Git repository.
   * @throws AuthenticationConfigException when there is an error while creating the authentication credentials to
   *                                       call remote Git.
   * @throws IllegalArgumentException      when the fetched application json or file path is invalid.
   */
  PullAppResponse<?> pull(ApplicationReference appRef, RepositoryConfig repoConfig) throws PullFailureException,
    NotFoundException, AuthenticationConfigException;
}
