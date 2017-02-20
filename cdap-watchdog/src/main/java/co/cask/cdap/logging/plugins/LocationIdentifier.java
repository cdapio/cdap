/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.plugins;

import java.util.Objects;

/**
 * Identifier for location of logs
 */
public class LocationIdentifier {
  private final String namespaceId;
  private final String applicationId;

  public LocationIdentifier(String namespaceId, String applicationId) {
    this.namespaceId = namespaceId;
    this.applicationId = applicationId;
  }

  public String getNamespaceId() {
    return namespaceId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LocationIdentifier that = (LocationIdentifier) o;

    return (Objects.equals(namespaceId, that.namespaceId) && Objects.equals(applicationId, that.applicationId));
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceId, applicationId);
  }
}
