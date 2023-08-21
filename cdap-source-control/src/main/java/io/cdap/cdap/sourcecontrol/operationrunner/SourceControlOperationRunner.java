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

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.ReadonlyArtifactRepositoryAccessor;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.sourcecontrol.PullAppDryrunResponse;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.SourceControlException;

/**
 * An interface encapsulating all operations needed for source control management.
 */
public interface SourceControlOperationRunner extends Service {
  /**
   * Push an application config to remote git repository.
   *
   * @param pushAppOperationRequest {@link PushAppOperationRequest} of the application to be pushed
   * @return file-paths and file-hashes for the updated configs.
   * @throws NoChangesToPushException      if there is no effective changes on the config file to commit
   * @throws AuthenticationConfigException when there is an error while creating the authentication credentials to
   *                                       call remote Git.
   * @throws SourceControlException when the push operation fails for any other reason.
   */
  PushAppResponse push(PushAppOperationRequest pushAppOperationRequest) throws NoChangesToPushException,
    AuthenticationConfigException;

  /**
   * Gets an application spec from a Git repository.
   *
   * @param pulAppOperationRequest The {@link PulAppOperationRequest} of the application to pull from
   * @return the details of the pulled application.
   * @throws NotFoundException             when the requested application is not found in the Git repository.
   * @throws AuthenticationConfigException when there is an error while creating the authentication credentials to
   *                                       call remote Git.
   * @throws IllegalArgumentException      when the fetched application json or file path is invalid.
   * @throws SourceControlException when the operation fails for any other reason.
   */
  PullAppResponse<?> pull(PulAppOperationRequest pulAppOperationRequest) throws NotFoundException,
    AuthenticationConfigException;

  PullAppDryrunResponse pullAndDryrun(
      ApplicationId appId,
      PulAppOperationRequest pulAppOperationRequest,
      ReadonlyArtifactRepositoryAccessor artifactRepository
  ) throws Exception;

  /**
   * Lists application configs found in repository.
   *
   * @param nameSpaceRepository {@link NamespaceRepository} for the repository to be listed
   * @return Name and git-file-hashes for the detected config files.
   * @throws AuthenticationConfigException when there is an error while creating the authentication credentials to
   *                                       call remote Git.
   * @throws NotFoundException when the given path-prefix is missing in the repository.
   * @throws SourceControlException when the list operation fails for any other reason.
   */
  RepositoryAppsResponse list(NamespaceRepository nameSpaceRepository) throws AuthenticationConfigException,
    NotFoundException;
}
