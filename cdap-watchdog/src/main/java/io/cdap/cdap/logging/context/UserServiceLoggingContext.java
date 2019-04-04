/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

import javax.annotation.Nullable;

/**
 * Logging Context for Services defined by users.
 */
public class UserServiceLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_USER_SERVICE_ID = ".userserviceid";
  public static final String TAG_HANDLER_ID = ".userhandlerid";

  public UserServiceLoggingContext(String namespaceId, String applicationId, String serviceId, String handlerId,
                                   String runId, @Nullable String instanceId) {
    super(namespaceId, applicationId, runId);
    setSystemTag(TAG_USER_SERVICE_ID, serviceId);
    setSystemTag(TAG_HANDLER_ID, handlerId);
    setInstanceId(instanceId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_USER_SERVICE_ID));
  }
}
