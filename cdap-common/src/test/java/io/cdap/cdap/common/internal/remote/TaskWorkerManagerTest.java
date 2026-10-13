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
import io.cdap.cdap.features.Feature;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link TaskWorkerManager}.
 */
public class TaskWorkerManagerTest {

  @Test
  public void testMultipleWorkersUseTheProxy() {
    CConfiguration cConf = newConf(true, true, 2);

    Assert.assertTrue(TaskWorkerManager.isEnabled(cConf));
    Assert.assertTrue(TaskWorkerManager.isProxyEnabled(cConf));
  }

  @Test
  public void testSingleWorkerKeepsLeasesWithoutTheProxy() {
    CConfiguration cConf = newConf(true, true, 1);

    Assert.assertTrue(TaskWorkerManager.isEnabled(cConf));
    Assert.assertFalse(TaskWorkerManager.isProxyEnabled(cConf));
  }

  @Test
  public void testFeatureFlagOffDisablesBoth() {
    CConfiguration cConf = newConf(false, true, 2);

    Assert.assertFalse(TaskWorkerManager.isEnabled(cConf));
    Assert.assertFalse(TaskWorkerManager.isProxyEnabled(cConf));
  }

  @Test
  public void testRbacOffDisablesBoth() {
    CConfiguration cConf = newConf(true, false, 2);

    Assert.assertFalse(TaskWorkerManager.isEnabled(cConf));
    Assert.assertFalse(TaskWorkerManager.isProxyEnabled(cConf));
  }

  private static CConfiguration newConf(boolean featureFlag, boolean rbac, int workers) {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean("feature." + Feature.RBAC_TASK_WORKER_MANAGER.getFeatureFlagString(),
        featureFlag);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, rbac);
    cConf.setInt(Constants.TaskWorker.CONTAINER_COUNT, workers);
    return cConf;
  }
}
