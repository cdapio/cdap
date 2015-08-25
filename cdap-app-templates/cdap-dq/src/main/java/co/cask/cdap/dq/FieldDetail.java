/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.dq;

import java.util.Objects;
import java.util.Set;

/**
 * FieldDetail objects should contain information about fieldName as well as
 * the aggregations that are applicable to the given field
 */
public class FieldDetail {
  private final String fieldName;
  private Set<AggregationTypeValue> aggregationTypeSet;

  public FieldDetail(String fieldName, Set<AggregationTypeValue> aggregationTypeSet) {
    this.fieldName = fieldName;
    this.aggregationTypeSet = aggregationTypeSet;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Set<AggregationTypeValue> getAggregationTypeSet() {
    return aggregationTypeSet;
  }

  public void addAggregations(Set<AggregationTypeValue> aggregations) {
    this.aggregationTypeSet.addAll(aggregations);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FieldDetail)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    FieldDetail fdObj = (FieldDetail) obj;
    return Objects.equals(fieldName, fdObj.getFieldName()) && aggregationTypeSet.equals(fdObj.getAggregationTypeSet());
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, aggregationTypeSet.hashCode());
  }

  @Override
  public String toString() {
    return "FieldDetail{" +
      "fieldName='" + fieldName + '\'' +
      ", aggregationTypeSet=" + aggregationTypeSet +
      '}';
  }
}
