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

import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;

/**
 * An interface encapsulating all operations needed for source control management
 */
public interface SourceControlOperationRunner {
  /**
   * @param appToPush {@link ApplicationDetail} to be pushed
   * @param commitDetails Details of commit author, committer and message
   * @return file-paths and file-hashes for the updated configs.
   * @throws PushFailureException when the push operation fails for any reason.
   */
  PushAppResponse push(NamespaceId namespace, RepositoryConfig repoConfig,
                       ApplicationDetail appToPush, CommitMeta commitDetails)
    throws PushFailureException, NoChangesToPushException, AuthenticationConfigException;
}
