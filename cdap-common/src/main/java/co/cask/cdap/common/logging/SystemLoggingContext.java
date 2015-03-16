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

package co.cask.cdap.common.logging;

/**
 * System Logging Context.
 */
public abstract class SystemLoggingContext extends AbstractLoggingContext {
  public static final String TAG_SYSTEM_ID = ".systemid";

  /**
   * Construct a SystemLoggingContext.
   * @param systemId system id
   */
  public SystemLoggingContext(final String systemId) {
    setSystemTag(TAG_SYSTEM_ID, systemId);
  }

  @Override
  public String getNamespacedLogBaseDir(String logBaseDir) {
    // cannot just call #getLogPathFragment here, because if called on a subclass' object, that would return
    // a wrong value
    return String.format("%s/%s", getSystemTag(TAG_SYSTEM_ID), logBaseDir);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s", getSystemTag(TAG_SYSTEM_ID));
  }

  @Override
  public String getLogPathFragment(String logBaseDir) {
    return String.format("%s/%s", getSystemTag(TAG_SYSTEM_ID), logBaseDir);
  }
}
