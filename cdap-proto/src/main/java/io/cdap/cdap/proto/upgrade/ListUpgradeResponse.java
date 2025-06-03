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

package io.cdap.cdap.proto.upgrade;

import java.util.List;
import java.util.Objects;

/**
 * Response for list upgrades HTTP call.
 */
public class ListUpgradeResponse {

  private final List<ApplicationUpgradeDetail> applicationUpgradeDetails;

  public ListUpgradeResponse(List<ApplicationUpgradeDetail> applicationUpgradeDetails) {
    this.applicationUpgradeDetails = applicationUpgradeDetails;
  }

  public List<ApplicationUpgradeDetail> getApplicationUpgradeDetails() {
    return applicationUpgradeDetails;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ListUpgradeResponse)) {
      return false;
    }
    ListUpgradeResponse that = (ListUpgradeResponse) o;
    return Objects.equals(applicationUpgradeDetails, that.applicationUpgradeDetails);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(applicationUpgradeDetails);
  }
}
