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

package io.cdap.cdap.data2.metadata.lineage.field;

import io.cdap.cdap.api.lineage.field.EndPoint;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represent a single field along with the EndPoint to which it belongs to.
 */
public class EndPointField {

  private final EndPoint endPoint;
  private final String field;
  private transient Integer hashCode;

  public EndPointField(EndPoint endPoint, @Nullable String field) {
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
    return Objects.equals(endPoint, that.endPoint)
        && Objects.equals(field, that.field);
  }

  @Override
  public int hashCode() {
    if (hashCode == null) {
      hashCode = Objects.hash(endPoint, field);
    }
    return hashCode;
  }

  @Override
  public String toString() {
    return "EndPointField{"
        + "endPoint=" + endPoint
        + ", field='" + field + '\''
        + '}';
  }

  /**
   * Checks for validity of an EndPointField.
   *
   * If in a pipeline a field is dropped, the source EndPointField corresponding to the dropped
   * field maps to an empty EndPointField of the form `EndPointField{endPoint=EndPoint{namespace='null',
   * name='null', properties='{}'}, field='null'}`. This method can be used to scan for such
   * EndPointFields.
   *
   * @return true if an EndPointField is valid, false otherwise
   */
  public boolean isValid() {
    return endPoint != null
        && endPoint.getName() != null
        && endPoint.getNamespace() != null
        && !endPoint.getProperties().isEmpty()
        && field != null;
  }
}
