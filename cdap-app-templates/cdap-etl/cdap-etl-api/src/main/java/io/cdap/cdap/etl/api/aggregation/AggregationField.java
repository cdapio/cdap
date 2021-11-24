/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.aggregation;

import io.cdap.cdap.etl.api.Field;

import java.util.Objects;

public class AggregationField extends Field {
  private String aggregateFunction;

  public AggregationField(String fieldName, String alias, String aggregateFunction) {
    super(fieldName, alias);
    this.aggregateFunction = aggregateFunction;
  }

  public String getAggregateFunction() {
    return aggregateFunction;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    AggregationField field = (AggregationField) that;
    return Objects.equals(fieldName, field.getFieldName()) &&
      Objects.equals(alias, field.getAlias()) &&
      Objects.equals(aggregateFunction, field.getAggregateFunction());
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, alias, aggregateFunction);
  }

  @Override
  public String toString() {
    return "AggregationField{" +
      "field='" + fieldName + '\'' +
      ", aggregateFunction='" + aggregateFunction + '\'' +
      ", alias='" + alias + '\'' +
      '}';
  }
}
