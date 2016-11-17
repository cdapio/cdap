/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations;

import java.util.Objects;

/**
 * Uniquely identifies an operational extension.
 */
public class OperationalExtensionId {
  private final String serviceName;
  private final String statType;

  public OperationalExtensionId(String serviceName, String statType) {
    this.serviceName = serviceName;
    this.statType = statType;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getStatType() {
    return statType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OperationalExtensionId that = (OperationalExtensionId) o;

    return Objects.equals(serviceName, that.serviceName) &&
      Objects.equals(statType, that.statType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceName, statType);
  }
}
