/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.app.store;

import io.cdap.cdap.proto.SourceControlMetadataRecord;
import java.util.List;

/**
 * Represents a response containing a list of source control metadata records, next page token and
 * last refresh time.
 */

public class ListSourceControlMetadataResponse {

  private final List<SourceControlMetadataRecord> apps;
  private final String nextPageToken;
  private final Long lastRefreshTime;

  /**
   * Constructs a ListSourceControlMetadataResponse object.
   *
   * @param apps            The list of source control metadata records.
   * @param nextPageToken   The token for fetching the next page of results.
   * @param lastRefreshTime The timestamp of the last refresh operation.
   */
  public ListSourceControlMetadataResponse(List<SourceControlMetadataRecord> apps,
      String nextPageToken, Long lastRefreshTime) {
    this.apps = apps;
    this.nextPageToken = nextPageToken;
    this.lastRefreshTime = lastRefreshTime;
  }

  public List<SourceControlMetadataRecord> getApps() {
    return apps;
  }

  public String getNextPageToken() {
    return nextPageToken;
  }

  public Long getLastRefreshTime() {
    return lastRefreshTime;
  }
}
