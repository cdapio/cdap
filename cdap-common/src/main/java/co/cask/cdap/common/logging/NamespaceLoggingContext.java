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
 * Namespace logging context.
 */
public abstract class NamespaceLoggingContext extends AbstractLoggingContext {
  public static final String TAG_NAMESPACE_ID = ".namespaceId";

  /**
   * Constructs AccountLoggingContext.
   * @param namespaceId namespace id
   */
  public NamespaceLoggingContext(final String namespaceId) {
    setSystemTag(TAG_NAMESPACE_ID, namespaceId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s", getSystemTag(TAG_NAMESPACE_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s", getSystemTag(TAG_NAMESPACE_ID));
  }
}
