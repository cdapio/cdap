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
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * An interface encapsulating all operations needed for source control management.
 */
public interface SourceControlOperationRunner extends Service {
  /**
   * Push an application config to remote git repository.
   *
   * @param pushRequest {@link PushAppOperationRequest} of the application to be pushed
   * @return file-paths and file-hashes for the updated configs.
   * @throws NoChangesToPushException      if there is no effective changes on the config file to commit
   * @throws AuthenticationConfigException when there is an error while creating the authentication credentials to
   *                                       call remote Git.
   * @throws SourceControlException when the push operation fails for any other reason.
   */
  PushAppResponse push(PushAppOperationRequest pushRequest) throws NoChangesToPushException,
    AuthenticationConfigException;

  /**
   * Pushes multiple application specs to repository. Rather than returning the PushAppResponses it
   * calls the given {@link Consumer} for each response.
   *
   * @param pushRequest The {@link MultiPushAppOperationRequest} of the applications to push
   * @param consumer {@link Consumer} that would be applied on each push response
   * @throws NotFoundException             when the requested application is not found.
   * @throws AuthenticationConfigException when there is an error while creating the authentication credentials to
   *                                       call remote Git.
   * @throws SourceControlException when the operation fails for any other reason.
   * @throws NoChangesToPushException when the apps being pushed have not changed
   */
  void push(MultiPushAppOperationRequest pushRequest, Consumer<Collection<PushAppResponse>> consumer)
      throws NotFoundException, AuthenticationConfigException, NoChangesToPushException;

  /**
   * Gets an application spec from a Git repository.
   *
   * @param pullRequest The {@link PullAppOperationRequest} of the application to pull from
   * @return the details of the pulled application.
   * @throws NotFoundException             when the requested application is not found in the Git repository.
   * @throws AuthenticationConfigException when there is an error while creating the authentication credentials to
   *                                       call remote Git.
   * @throws IllegalArgumentException      when the fetched application json or file path is invalid.
   * @throws SourceControlException when the operation fails for any other reason.
   */
  PullAppResponse<?> pull(PullAppOperationRequest pullRequest) throws NotFoundException,
    AuthenticationConfigException;

  /**
   * Pulls multiple application specs from repository. Rather than returning the specs it calls the
   * given {@link Consumer} for each spec.
   *
   * @param pullRequest The {@link MultiPullAppOperationRequest} of the applications to pull
   * @param consumer {@link Consumer} that would be applied on each spec
   * @throws NotFoundException             when the requested application is not found in the Git repository.
   * @throws AuthenticationConfigException when there is an error while creating the authentication credentials to
   *                                       call remote Git.
   * @throws IllegalArgumentException      when the fetched application json or file path is invalid.
   * @throws SourceControlException when the operation fails for any other reason.
   */
  void pull(MultiPullAppOperationRequest pullRequest, Consumer<PullAppResponse<?>> consumer)
    throws NotFoundException, AuthenticationConfigException;

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
