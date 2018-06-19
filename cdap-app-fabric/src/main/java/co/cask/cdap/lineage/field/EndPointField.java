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

package co.cask.cdap.lineage.field;

import co.cask.cdap.api.lineage.field.EndPoint;

import java.util.Objects;

/**
 * Represent a single field along with the EndPoint to which it belongs to.
 */
public class EndPointField {
  private final EndPoint endPoint;
  private final String field;

  EndPointField(EndPoint endPoint, String field) {
    this.endPoint = endPoint;
    this.field = field;
  }

  public EndPoint getEndPoint() {
    return endPoint;
  }

  public String getField() {
    return field;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EndPointField that = (EndPointField) o;
    return Objects.equals(endPoint, that.endPoint) &&
            Objects.equals(field, that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(endPoint, field);
  }
}
