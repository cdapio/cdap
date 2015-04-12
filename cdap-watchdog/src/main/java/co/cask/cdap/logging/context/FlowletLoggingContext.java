/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.common.logging.ApplicationLoggingContext;

/**
 * Flowlet logging context.
 */
public class FlowletLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_FLOW_ID = ".flowId";
  public static final String TAG_FLOWLET_ID = ".flowletId";

  /**
   * Constructs the FlowletLoggingContext.
   * @param namespaceId namespace id
   * @param applicationId application id
   * @param flowId flow id
   * @param flowletId flowlet id
   * @param runId run id
   * @param instanceId instance id
   */
  public FlowletLoggingContext(String namespaceId, String applicationId, String flowId, String flowletId,
                               String runId, String instanceId) {
    super(namespaceId, applicationId, runId);
    setSystemTag(TAG_FLOW_ID, flowId);
    setSystemTag(TAG_FLOWLET_ID, flowletId);
    setInstanceId(instanceId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_FLOW_ID));
  }

  @Override
  public String getLogPathFragment(String logBaseDir) {
    return String.format("%s/flow-%s", super.getLogPathFragment(logBaseDir), getSystemTag(TAG_FLOW_ID));
  }
}
