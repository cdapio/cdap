/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.context;

import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.common.logging.ApplicationLoggingContext;

/**
 * Logging Context for {@link Worker}
 */
public class WorkerLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_WORKER_ID = ".workerid";

  public WorkerLoggingContext(final String namespaceId, final String appId, final String workerId) {
    super(namespaceId, appId);
    setSystemTag(TAG_WORKER_ID, workerId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_WORKER_ID));
  }

  @Override
  public String getLogPathFragment(String logBaseDir) {
    return String.format("%s/worker-%s", super.getLogPathFragment(logBaseDir), getSystemTag(TAG_WORKER_ID));
  }
}
