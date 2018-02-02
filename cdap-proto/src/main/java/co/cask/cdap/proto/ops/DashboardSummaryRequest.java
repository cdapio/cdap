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

import java.util.List;

/**
 * Represents the request for a dashboard summary in an HTTP request.
 */
public class DashboardSummaryRequest extends ProgramRunOperationRequest {
  private final int resolution;

  public DashboardSummaryRequest(long startTs, long endTs, String principal, List<String> namespaces, int resolution) {
    super(startTs, endTs, principal, namespaces);
    this.resolution = resolution;
  }

  public int getResolution() {
    return resolution;
  }
}
