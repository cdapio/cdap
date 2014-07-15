/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.logging.context;

import com.continuuity.common.logging.ApplicationLoggingContext;

/**
 * Flowlet logging context.
 */
public class FlowletLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_FLOW_ID = ".flowId";
  public static final String TAG_FLOWLET_ID = ".flowletId";

  /**
   * Constructs the FlowletLoggingContext.
   * @param accountId account id
   * @param applicationId application id
   * @param flowId flow id
   * @param flowletId flowlet id
   */
  public FlowletLoggingContext(final String accountId,
                               final String applicationId,
                               final String flowId,
                               final String flowletId) {
    super(accountId, applicationId);
    setSystemTag(TAG_FLOW_ID, flowId);
    setSystemTag(TAG_FLOWLET_ID, flowletId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_FLOW_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/flow-%s", super.getLogPathFragment(), getSystemTag(TAG_FLOW_ID));
  }
}
