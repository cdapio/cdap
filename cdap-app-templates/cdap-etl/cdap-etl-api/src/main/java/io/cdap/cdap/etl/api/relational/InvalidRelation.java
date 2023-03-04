/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.relational;

/**
 * Invalid relation specification
 */

import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.WindowAggregationDefinition;
import java.util.Map;

public class InvalidRelation implements Relation {

  private final String errorMsg;

  public InvalidRelation(String errorMsg) {
    this.errorMsg = errorMsg;
  }

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public String getValidationError() {
    return errorMsg;
  }

  @Override
  public Relation setColumn(String s, Expression expression) {
    return this;
  }

  @Override
  public Relation dropColumn(String s) {
    return this;
  }

  @Override
  public Relation select(Map<String, Expression> map) {
    return this;
  }

  @Override
  public Relation filter(Expression expression) {
    return this;
  }

  @Override
  public Relation groupBy(GroupByAggregationDefinition aggregationDefinition) {
    return this;
  }

  @Override
  public Relation deduplicate(DeduplicateAggregationDefinition aggregationDefinition) {
    return this;
  }

  @Override
  public Relation window(WindowAggregationDefinition aggregationDefinition) {
    return this;
  }
}
