/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.features.Feature;

/**
 * Decides how task workers are shared between namespaces.
 *
 * <p>There are two checks because a single task worker has nothing to route: it still leases
 * itself to one namespace at a time ({@link #isStickyLeaseEnabled}), but no task worker manager
 * is deployed and clients call it directly ({@link #isEnabled}).
 */
public final class TaskWorkerManagerUtil {

  private TaskWorkerManagerUtil() {
    // Utility class.
  }

  /**
   * Returns whether task workers admit tasks under a namespace lease at full concurrency.
   *
   * @param cConf the CDAP configuration to read the feature flag and RBAC setting from
   * @return true when both the feature flag and instance level RBAC are enabled
   */
  public static boolean isStickyLeaseEnabled(CConfiguration cConf) {
    return Feature.TASK_WORKER_MANAGER.isEnabled(new DefaultFeatureFlagsProvider(cConf))
        && cConf.getBoolean(Constants.Security.Authorization.ENABLED);
  }

  /**
   * Returns whether the task worker manager is deployed and task worker requests go through it.
   *
   * @param cConf the CDAP configuration to read the feature flag, RBAC setting and pool size from
   * @return true when {@link #isStickyLeaseEnabled} holds and there is more than one task worker
   */
  public static boolean isEnabled(CConfiguration cConf) {
    return isStickyLeaseEnabled(cConf) && cConf.getInt(Constants.TaskWorker.CONTAINER_COUNT) > 1;
  }
}
