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
 * Decides whether task workers run under namespace leases ({@link #isEnabled}) and whether clients
 * reach them through the Task Worker Manager proxy ({@link #isProxyEnabled}). Clients, workers and
 * the proxy launcher all read these, so they can't disagree.
 *
 * <p>RBAC is read from {@link Constants.Security.Authorization#ENABLED} rather than the namespaced
 * service accounts flag, because instance-level authorization is what CDF enables the proxy for.
 */
public final class TaskWorkerManager {

  private TaskWorkerManager() {
    // Utility class.
  }

  /**
   * Returns whether task workers admit tasks under a namespace lease at full concurrency.
   *
   * @param cConf the CDAP configuration to read the feature flag and RBAC setting from
   * @return true when both the feature flag and instance-level RBAC are enabled
   */
  public static boolean isEnabled(CConfiguration cConf) {
    return Feature.RBAC_TASK_WORKER_MANAGER.isEnabled(new DefaultFeatureFlagsProvider(cConf))
        && cConf.getBoolean(Constants.Security.Authorization.ENABLED);
  }

  /**
   * Returns whether task worker traffic goes through the proxy. A single worker leaves the proxy
   * nothing to route, so clients call it directly and its lease enforces isolation.
   *
   * @param cConf the CDAP configuration to read the feature flag, RBAC setting and pool size from
   * @return true when {@link #isEnabled} holds and there is more than one task worker
   */
  public static boolean isProxyEnabled(CConfiguration cConf) {
    return isEnabled(cConf) && cConf.getInt(Constants.TaskWorker.CONTAINER_COUNT) > 1;
  }
}
