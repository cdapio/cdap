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

import co.cask.cdap.common.logging.ApplicationLoggingContext;

import javax.annotation.Nullable;

/**
 * Logging context for the Workflow.
 */
public class WorkflowLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_WORKFLOW_ID = ".workflowId";

  /**
   * Constructs ApplicationLoggingContext.
   *
   * @param namespaceId   namespace id
   * @param applicationId application id
   * @param workflowId    workflow id
   * @param runId         run id of the application
   */
  public WorkflowLoggingContext(String namespaceId, String applicationId, String workflowId, String runId,
                                @Nullable String adapterId) {
    super(namespaceId, applicationId, runId);
    setSystemTag(TAG_WORKFLOW_ID, workflowId);
    if (adapterId != null) {
      setAdapterId(adapterId);
    }
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_WORKFLOW_ID));
  }

  @Override
  public String getLogPathFragment(String logBaseDir) {
    return String.format("%s/workflow-%s", super.getLogPathFragment(logBaseDir), getSystemTag(TAG_WORKFLOW_ID));
  }
}
