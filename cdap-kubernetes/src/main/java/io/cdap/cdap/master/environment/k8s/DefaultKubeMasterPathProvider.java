/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;

public class DefaultKubeMasterPathProvider implements KubeMasterPathProvider {

  @Override
  public String getMasterPath() {
    // Spark master base path
    String master;
    // apiClient.getBasePath() returns path similar to https://10.8.0.1:443
    try {
      ApiClient apiClient = Config.defaultClient();
      master = apiClient.getBasePath();
    } catch (Exception e) {
      // should not happen
      throw new RuntimeException("Exception while getting kubernetes master path", e);
    }
    return master;
  }
}
