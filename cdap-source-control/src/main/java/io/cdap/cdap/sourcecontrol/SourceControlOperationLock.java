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

package io.cdap.cdap.sourcecontrol;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SourceControlManagement;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SourceControlOperationLock {
  private final boolean remoteRunnerEnabled;
  private final Semaphore operationSemaphore;

  private static final int LOCK_TIMEOUT = 5;

  @Inject
  public SourceControlOperationLock(CConfiguration cConf){
    remoteRunnerEnabled = cConf.getBoolean(Constants.TaskWorker.POOL_ENABLE);
    int maxOperation = cConf.getInt(SourceControlManagement.MAX_PARALLEL_GIT_OPERATION_APPFABRIC);
    operationSemaphore = new Semaphore(maxOperation, true);
  }

  public void acquire() {
    if(!remoteRunnerEnabled) {
      try {
        if (!operationSemaphore.tryAcquire(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
          throw new RuntimeException("Timeout while trying to acquire git operation lock");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for git operation lock.", e);
      }
    }
  }

  public void release() {
    if (remoteRunnerEnabled)
      operationSemaphore.release();
  }

}
