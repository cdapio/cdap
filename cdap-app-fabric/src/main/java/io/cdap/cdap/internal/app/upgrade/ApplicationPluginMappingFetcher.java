/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.upgrade;

import io.cdap.cdap.proto.id.NamespaceId;
import java.util.List;

/**
 * Fetcher for application to plugin mapping.
 */
public interface ApplicationPluginMappingFetcher {

  /**
   * Fetches application to plugin mapping for a namespace.
   *
   * @param namespace the namespace for which mapping will be fetched.
   * @return a list of type {@link ApplicationPluginMapping}. An empty list will be returned if no
   *     applications are found.
   * @throws Exception the exception during fetching of application plugin mapping.
   */
  List<ApplicationPluginMapping> fetchApplicationPluginMapping(NamespaceId namespace)
      throws Exception;

}
