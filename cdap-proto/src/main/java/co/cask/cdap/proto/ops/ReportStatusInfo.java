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

package co.cask.cdap.proto.ops;

/**
 * Represents the status information of a report.
 */
public class ReportStatusInfo {
  private final String id;
  private final long created;
  private final ReportStatus status;

  public ReportStatusInfo(String id, long created, ReportStatus status) {
    this.id = id;
    this.created = created;
    this.status = status;
  }

  public String getId() {
    return id;
  }

  public long getCreated() {
    return created;
  }

  public ReportStatus getStatus() {
    return status;
  }
}
