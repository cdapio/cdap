/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import java.util.List;

/**
 * The response payload for the application summary endpoint.
 */
public class AppSummaryResponse {

  private final List<AppSummaryRecord> apps;
  private final String nextPageToken;

  /**
   * Constructs an instance of {@link AppSummaryResponse}.
   *
   * @param apps          list of application summaries
   * @param nextPageToken token for the next page of results, or null if none
   */
  public AppSummaryResponse(List<AppSummaryRecord> apps, String nextPageToken) {
    this.apps = apps;
    this.nextPageToken = nextPageToken;
  }

  /**
   * Returns the list of application summaries.
   */
  public List<AppSummaryRecord> getApps() {
    return apps;
  }

  /**
   * Returns the token for the next page of results, or null if there are no more pages.
   */
  public String getNextPageToken() {
    return nextPageToken;
  }
}
