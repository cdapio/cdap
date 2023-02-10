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

package io.cdap.cdap.internal.app.sourcecontrol;

import io.cdap.cdap.sourcecontrol.CommitMeta;

import java.util.List;

/**
 * An interface encapsulating all operations needed for source control management
 */
public interface SourceControlOperationRunner {
  /**
   * @param appsToPush List of app names and configs to be pushed
   * @param commitDetails Details of commit author, committer and message
   * @return file-paths and file-hashes for the updated configs.
   * @throws PushFailureException when the push operation fails for any reason.
   */
  PushAppsResponse push(List<AppDetailsToPush> appsToPush, CommitMeta commitDetails) throws PushFailureException;
}
