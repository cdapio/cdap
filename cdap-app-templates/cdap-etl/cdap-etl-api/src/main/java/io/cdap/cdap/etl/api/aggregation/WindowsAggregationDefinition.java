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
import java.util.Objects;


/**
 * Specifies how a window operation should be executed.
 */
public class WindowsAggregationDefinition extends AggregationDefinition {
  private WindowsAggregationDefinition(List<Expression> windowsExpressions,
                                         Map<String, Expression> selectExpressions) {
        super(windowsExpressions, selectExpressions);
  }
    /**
     * @return A builder to create a WindowsAggregationDefinition.
     */
  public static Builder builder() {
        return new Builder();
    }

  public enum FilterFunction {
    RANK,
    DENSE_RANK,
    PERCENT_RANK,
    N_TILE,
    ROW_NUMBER,
    MEDIAN,
    CONTINUOUS_PERCENTILE,
    DISCRETE_PERCENTILE,
    LEAD,
    LAG,
    FIRST,
    LAST,
    CUMULATIVE_DISTRIBUTION,
    ACCUMULATE;
  }

  public static class FilterExpression {
    private final Expression expression;
    private final FilterFunction filterFunction;

    /**
     * Creates a new {@link FilterExpression} using the specified {@link Expression} and {@link FilterFunction}.
     * @param expression the expression using which filtering of duplicate records is to be performed
     * @param filterFunction the function to be applied on the duplicate records to select a single record
     */
    public FilterExpression(Expression expression, FilterFunction filterFunction) {
        this.expression = expression;
        this.filterFunction = filterFunction;
    }

    /**
     *
     * @return the expression used for filtering duplicate records
     */
    public Expression getExpression() {
        return expression;
    }

        /**
         *
         * @return the function applied to select a single record from duplicate records
         */
        public FilterFunction getFilterFunction() {
            return filterFunction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FilterExpression that = (FilterExpression) o;
            return Objects.equals(getExpression(), that.getExpression())
              && getFilterFunction() == that.getFilterFunction();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getExpression(), getFilterFunction());
        }
    }
    /**
     * Builds a WindowsAggregationDefinition using fields to group by and fields to select.
     * The fields to select must be specified, while the fields to group by are optional.
     */
    public static class Builder {
        private List<Expression> windowsExpressions;
        private Map<String, Expression> selectExpressions;

        public Builder() {
            windowsExpressions = Collections.emptyList();
            selectExpressions = Collections.emptyMap();
        }

        /**
         * Sets the list of expressions to perform grouping by to the specified list.
         * Any existing group by expression list is overwritten.
         * @param windowsExpressions list of {@link Expression}s to group by.
         * @return a {@link Builder} with the currently built {@link WindowsAggregationDefinition}.
         */
        public Builder window(List<Expression> windowsExpressions) {
            this.windowsExpressions = windowsExpressions;
            return this;
        }

        /**
         * Sets the list of expressions to perform grouping by to the specified expressions.
         * Any existing group by expression list is overwritten.
         * @param windowsExpressions {@link Expression}s to group by.
         * @return a {@link Builder} with the currently built {@link WindowsAggregationDefinition}.
         */
        public Builder window(Expression... windowsExpressions) {
            return window(Arrays.asList(windowsExpressions));
        }

        /**
         * Sets the list of expressions to select to the specified list of expressions.
         * Any existing list of select expressions is overwritten.
         * @param selectExpressions list of {@link Expression}s to select.
         * @return a {@link Builder} with the currently built {@link WindowsAggregationDefinition}.
         */
        public Builder select(Map<String, Expression> selectExpressions) {
            this.selectExpressions = selectExpressions;
            return this;
        }

        /**
         * Sets the list of expressions to select to the specified expressions.
         * Any existing list of select expressions is overwritten.
         * @param key the key to use for this expression
         * @param expression expression to use
         * @return a {@link Builder} with the currently built {@link WindowsAggregationDefinition}.
         */
        public Builder select(String key, Expression expression) {
            this.selectExpressions.put(key, expression);
            return this;
        }

        /**
         * Builds a WindowsAggregationDefinition.
         * @return A WindowsAggregationDefinition object.
         * @throws IllegalStateException in case the fields to select are not specified.
         */
        public WindowsAggregationDefinition build() {
            if (selectExpressions.isEmpty()) {
                throw new IllegalStateException("Can't build a WindowsAggregationDefinition without select fields");
            }

            return new WindowsAggregationDefinition(windowsExpressions, selectExpressions);
        }
    }

}
