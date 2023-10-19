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

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import java.util.Collection;

/**
 * Provides various helper methods for source control operations to operate on applications.
 * Would be implemented for both running in app-fabric and running in workers.
 */
public interface ApplicationManager {

  /**
   * Deploys a given app with the given pull app details.
   *
   * @param appRef the {@link ApplicationReference} for the app to be deployed
   * @param pullDetails {@link PullAppResponse} which includes the app spec and the git hash.
   * @return The {@link ApplicationId} for the deployed version
   * @throws SourceControlException for any failure. We wrap all failures to
   *     {@link SourceControlException}
   */
  ApplicationId deployApp(ApplicationReference appRef, PullAppResponse<?> pullDetails)
      throws SourceControlException;

  /**
   * Mark the given list of app-versions as the latest. Only the latest version for any app is
   * runnable.
   *
   * @param appIds List of {@link ApplicationId} to be marked latest
   * @throws SourceControlException for any failure. We wrap all failures to
   *     {@link SourceControlException}
   */
  void markAppVersionsLatest(Collection<ApplicationId> appIds) throws SourceControlException;
}
