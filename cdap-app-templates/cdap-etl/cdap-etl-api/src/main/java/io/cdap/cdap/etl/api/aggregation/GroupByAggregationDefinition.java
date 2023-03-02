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

import io.cdap.cdap.etl.api.relational.Expression;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Specifies how a group by operation should be executed.
 */
public class GroupByAggregationDefinition extends AggregationDefinition {

  private GroupByAggregationDefinition(List<Expression> groupByExpressions,
      Map<String, Expression> selectExpressions) {
    super(groupByExpressions, selectExpressions);
  }

  /**
   * @return A builder to create a GroupByAggregationDefinition.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a GroupByAggregationDefinition using fields to group by and fields to select. The fields
   * to select must be specified, while the fields to group by are optional.
   */
  public static class Builder {

    private List<Expression> groupByExpressions;
    private Map<String, Expression> selectExpressions;

    public Builder() {
      groupByExpressions = Collections.emptyList();
      selectExpressions = Collections.emptyMap();
    }

    /**
     * Sets the list of expressions to perform grouping by to the specified list. Any existing group
     * by expression list is overwritten.
     *
     * @param groupByExpressions list of {@link Expression}s to group by.
     * @return a {@link Builder} with the currently built {@link GroupByAggregationDefinition}.
     */
    public Builder groupBy(List<Expression> groupByExpressions) {
      this.groupByExpressions = groupByExpressions;
      return this;
    }

    /**
     * Sets the list of expressions to perform grouping by to the specified expressions. Any
     * existing group by expression list is overwritten.
     *
     * @param groupByExpressions {@link Expression}s to group by.
     * @return a {@link Builder} with the currently built {@link GroupByAggregationDefinition}.
     */
    public Builder groupBy(Expression... groupByExpressions) {
      return groupBy(Arrays.asList(groupByExpressions));
    }

    /**
     * Sets the list of expressions to select to the specified list of expressions. Any existing
     * list of select expressions is overwritten.
     *
     * @param selectExpressions list of {@link Expression}s to select.
     * @return a {@link Builder} with the currently built {@link GroupByAggregationDefinition}.
     */
    public Builder select(Map<String, Expression> selectExpressions) {
      this.selectExpressions = selectExpressions;
      return this;
    }

    /**
     * Sets the list of expressions to select to the specified expressions. Any existing list of
     * select expressions is overwritten.
     *
     * @param key the key to use for this expression
     * @param expression expression to use
     * @return a {@link Builder} with the currently built {@link GroupByAggregationDefinition}.
     */
    public Builder select(String key, Expression expression) {
      this.selectExpressions.put(key, expression);
      return this;
    }

    /**
     * Builds a GroupByAggregationDefinition.
     *
     * @return A GroupByAggregationDefinition object.
     * @throws IllegalStateException in case the fields to select are not specified.
     */
    public GroupByAggregationDefinition build() {
      if (selectExpressions.isEmpty()) {
        throw new IllegalStateException(
            "Can't build a GroupByAggregationDefinition without select fields");
      }

      return new GroupByAggregationDefinition(groupByExpressions, selectExpressions);
    }
  }
}
