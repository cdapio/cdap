/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.context;

import io.cdap.cdap.common.logging.NamespaceLoggingContext;

import javax.annotation.Nullable;

/**
 * Application logging context.
 */
public abstract class ApplicationLoggingContext extends NamespaceLoggingContext {
  public static final String TAG_APPLICATION_ID = ".applicationId";
  public static final String TAG_RUN_ID = ".runId";
  public static final String TAG_INSTANCE_ID = ".instanceId";

  /**
   * Constructs ApplicationLoggingContext.
   * @param namespaceId namespace id
   * @param applicationId application id
   * @param runId run id of the application
   */
  protected ApplicationLoggingContext(String namespaceId, String applicationId, String runId) {
    super(namespaceId);
    setSystemTag(TAG_APPLICATION_ID, applicationId);
    setSystemTag(TAG_RUN_ID, runId);
  }

  protected void setInstanceId(@Nullable String instanceId) {
    setSystemTag(TAG_INSTANCE_ID, instanceId == null ? "0" : instanceId);
  }

  @Override
  public String getLogPartition() {
    return super.getLogPartition() + String.format(":%s", getSystemTag(TAG_APPLICATION_ID));
  }
}
