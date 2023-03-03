/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol.operationrunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Response class encapsulating information about a list of applications found in repository
 */
public class RepositoryAppsResponse {
  private final List<RepositoryApp> apps;

  public RepositoryAppsResponse(List<RepositoryApp> apps) {
    this.apps = Collections.unmodifiableList(new ArrayList<>(apps));
  }

  public List<RepositoryApp> getApps() {
    return apps;
  }
}
