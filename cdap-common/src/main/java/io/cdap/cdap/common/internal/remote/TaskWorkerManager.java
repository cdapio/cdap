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
 * Single source of truth for whether the centralized Task Worker Manager proxy is active on this CDAP
 * instance.
 *
 * <p>Both halves of the proxy path depend on this answer and they must never disagree:
 * <ul>
 *   <li>{@link RemoteClientFactory} and {@link RemoteTaskExecutor} use it to decide whether
 *       AppFabric addresses {@code task.worker.manager} instead of {@code task.worker}.</li>
 *   <li>{@link TaskWorkerHttpHandlerInternal} uses it to decide whether a task worker pod runs
 *       under a namespace lease at full concurrency, or stays clamped to a single task.</li>
 * </ul>
 *
 * <p>If the client side routed through the proxy while the worker side stayed clamped, the proxy
 * would dispatch up to {@code task.worker.request.limit} concurrent tasks to a pod that accepts
 * one, and every surplus request would bounce. Keeping the predicate in one place makes that
 * mismatch impossible to introduce by editing only one of the call sites.
 *
 * <p>The instance-level RBAC check is deliberately {@link Constants.Security.Authorization#ENABLED}
 * rather than the namespaced service accounts feature flag. Authorization is what CDF uses to
 * decide whether to deploy the {@code task.worker.manager} service at all, so it is the condition that
 * determines whether the proxy physically exists in the cluster.
 */
public final class TaskWorkerManager {

  private TaskWorkerManager() {
    // Utility class.
  }

  /**
   * Returns whether task worker traffic is routed through the Task Worker Manager proxy.
   *
   * @param cConf the CDAP configuration to read the feature flag and RBAC setting from
   * @return true when both the feature flag and instance-level RBAC are enabled
   */
  public static boolean isEnabled(CConfiguration cConf) {
    return Feature.RBAC_TASK_WORKER_MANAGER.isEnabled(new DefaultFeatureFlagsProvider(cConf))
        && cConf.getBoolean(Constants.Security.Authorization.ENABLED);
  }
}
