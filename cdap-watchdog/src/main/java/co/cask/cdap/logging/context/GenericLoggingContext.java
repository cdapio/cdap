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
 * A logging context when the type of entity is not known. This logging context has limited functionality.
 */
public class GenericLoggingContext extends ApplicationLoggingContext {
  public static final String TAG_ENTITY_ID = ".entityId";

  /**
   * Constructs the GenericLoggingContext.
   * @param namespaceId namespace id
   * @param applicationId application id
   * @param entityId flow entity id
   */
  public GenericLoggingContext(final String namespaceId,
                               final String applicationId,
                               final String entityId) {
    super(namespaceId, applicationId);
    setSystemTag(TAG_ENTITY_ID, entityId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_ENTITY_ID));
  }

  @Override
  public String getLogPathFragment(String logBaseDir) {
    throw new UnsupportedOperationException("GenericLoggingContext does not support this");
  }
}
