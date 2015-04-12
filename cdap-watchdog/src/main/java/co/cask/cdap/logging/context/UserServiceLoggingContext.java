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
 * Logging Context for Services defined by users.
 */
public class UserServiceLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_USERSERVICE_ID = ".userserviceid";
  public static final String TAG_RUNNABLE_ID = ".userrunnableid";

  public UserServiceLoggingContext(final String namespaceId,
                                   final String applicationId,
                                   final String serviceId,
                                   final String runnableId) {
    super(namespaceId, applicationId);
    setSystemTag(TAG_USERSERVICE_ID, serviceId);
    setSystemTag(TAG_RUNNABLE_ID, runnableId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_USERSERVICE_ID));
  }

  @Override
  public String getLogPathFragment(String logBaseDir) {
    return String.format("%s/userservice-%s", super.getLogPathFragment(logBaseDir), getSystemTag(TAG_USERSERVICE_ID));
  }

}
