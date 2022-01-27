/*
 * Copyright © 2016-2022 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.proto;

import java.util.List;

/**
 * Result for getting Paginated Application Records
 */
public class PaginatedApplicationRecords {

  public List<ApplicationRecord> getApplications() {
    return applications;
  }

  public String getNextPageToken() {
    return nextPageToken;
  }

  private final List<ApplicationRecord> applications;
  private final String nextPageToken;

  public PaginatedApplicationRecords(List<ApplicationRecord> applications, String nextPageToken) {
    this.applications = applications;
    this.nextPageToken = nextPageToken;
  }
}
