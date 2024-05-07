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

/**
 * Represents a response containing a single source control metadata record and last refresh time.
 */
public class SingleSourceControlMetadataResponse {

  private final SourceControlMetadataRecord app;
  private final Long lastRefreshTime;

  /**
   * Constructs a SingleSourceControlMetadataResponse object.
   *
   * @param app             The source control metadata record.
   * @param lastRefreshTime The timestamp of the last refresh operation.
   */
  public SingleSourceControlMetadataResponse(SourceControlMetadataRecord app,
      Long lastRefreshTime) {
    this.app = app;
    this.lastRefreshTime = lastRefreshTime;
  }

  public SourceControlMetadataRecord getApp() {
    return app;
  }

  public Long getLastRefreshTime() {
    return lastRefreshTime;
  }
}
