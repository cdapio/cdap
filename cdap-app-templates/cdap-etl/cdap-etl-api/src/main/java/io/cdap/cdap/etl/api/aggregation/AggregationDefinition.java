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

package io.cdap.cdap.etl.api.aggregation;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.etl.api.relational.Expression;

import java.util.Collections;
import java.util.List;

/**
 * Specifies how an aggregation should be executed.
 */
@Beta
public abstract class AggregationDefinition {
  private final List<Expression> groupByExpressions;
  private final List<Expression> selectExpressions;

  /**
   * Creates a default {@link AggregationDefinition} object with empty lists for group by and select expressions.
   */
  protected AggregationDefinition() {
    groupByExpressions = Collections.emptyList();
    selectExpressions = Collections.emptyList();
  }

  /**
   * Creates an {@link AggregationDefinition} object with the specified lists of expression for grouping and selection.
   * @param groupByExpressions A {@link List} of {@link Expression} objects for grouping.
   * @param selectExpressions A {@link List} of {@link Expression} objects for selection.
   */
  protected AggregationDefinition(List<Expression> groupByExpressions, List<Expression> selectExpressions) {
    this.groupByExpressions = Collections.unmodifiableList(groupByExpressions);
    this.selectExpressions = Collections.unmodifiableList(selectExpressions);
  }

  /**
   * Get the list of expressions on which grouping is to be performed.
   * @return {@link List} of {@link Expression} objects used for grouping.
   */
  public List<Expression> getGroupByExpressions() {
    return groupByExpressions;
  }

  /**
   * Get the list of expressions which are to be selected.
   * @return {@link List} of {@link Expression} objects to be selected.
   */
  public List<Expression> getSelectExpressions() {
    return selectExpressions;
  }
}
