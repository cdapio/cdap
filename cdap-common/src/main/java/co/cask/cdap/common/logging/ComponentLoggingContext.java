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
 * Component Logging Context.
 */
public class ComponentLoggingContext extends SystemLoggingContext {
  public static final String TAG_COMPONENT_ID = ".componentId";

  /**
   * Constructs ComponentLoggingContext.
   * @param systemId system id
   * @param componentId component id
   */
  public ComponentLoggingContext(final String systemId, final String componentId) {
    super(systemId);
    setSystemTag(TAG_COMPONENT_ID, componentId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_COMPONENT_ID));
  }

  @Override
  public String getLogPathFragment(String logBaseDir) {
    return String.format("%s/%s", super.getLogPathFragment(logBaseDir), getSystemTag(TAG_COMPONENT_ID));
  }
}
