/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.proto;

import javax.annotation.Nullable;

/**
 * Represents the status information of a report.
 */
public class ReportStatusInfo extends ReportMetaInfo {
  private final String id;

  public ReportStatusInfo(String id, String name, @Nullable String description,
                          long created, @Nullable Long expiry, ReportStatus status) {
    super(name, description, created, expiry, status);
    this.id = id;
  }

  public ReportStatusInfo(String id, ReportMetaInfo metaInfo) {
    this(id, metaInfo.getName(), metaInfo.getDescription(), metaInfo.getCreated(), metaInfo.getExpiry(),
         metaInfo.getStatus());
  }

  /**
   * @return report ID
   */
  public String getId() {
    return id;
  }
}
