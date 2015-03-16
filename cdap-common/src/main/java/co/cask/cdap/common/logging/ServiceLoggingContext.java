/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.logging;

/**
 * Service Logging Context
 */
public class ServiceLoggingContext extends ComponentLoggingContext {
  public static final String TAG_SERVICE_ID = ".serviceId";

  /**
   * Construct ServiceLoggingContext.
   * @param systemId system id
   * @param componentId component id
   * @param serviceId service id
   */
  public ServiceLoggingContext(final String systemId, final String componentId, final String serviceId) {
    super(systemId, componentId);
    setSystemTag(TAG_SERVICE_ID, serviceId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_SERVICE_ID));
  }

  @Override
  public String getLogPathFragment(String logBaseDir) {
    return String.format("%s/service-%s", super.getLogPathFragment(logBaseDir), getSystemTag(TAG_SERVICE_ID));
  }

}
