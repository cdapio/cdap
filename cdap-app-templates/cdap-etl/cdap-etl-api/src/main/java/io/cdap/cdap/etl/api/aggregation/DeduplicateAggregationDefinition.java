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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Specifies how a deduplicate operation should be executed. A deduplication is an operation that
 * takes in a set of rows and for each group of rows for which the values for certain group by
 * fields are the same, outputs a single row. This single row can be determined by performing an
 * operation on the set of rows which returns a single row. For instance, to select the record with
 * the latest <code>creation_time</code>, the filter expression used would be
 * (<code>creation_time</code>, <code>MAX</code>). Currently the two supported operations are
 * returning the row with the MIN or MAX value of a particular expression. If the first filter
 * expression in the list returns more than one row, the next filter expression will be used as a
 * tie breaker, and so on until the list of filter expressions is exhausted.
 */
public class DeduplicateAggregationDefinition extends AggregationDefinition {

  List<FilterExpression> filterExpressions;

  private DeduplicateAggregationDefinition(List<Expression> groupByExpressions,
      Map<String, Expression> selectExpressions,
      List<FilterExpression> filterExpressions) {
    super(groupByExpressions, selectExpressions);
    this.filterExpressions = filterExpressions;
  }

  /**
   * @return A builder to create a DeduplicateAggregationDefinition.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Get the list of expressions using which a single record will be selected from a group of
   * records for deduplication. Each entry in the list is defined as a pair of an {@link Expression}
   * and {@link FilterFunction}, specifying the function to be executed on the expression.
   *
   * @return A {@link List} of {@link FilterExpression} objects, each of which is a pair of an
   *     {@link Expression} and a {@link FilterFunction}.
   */
  public List<FilterExpression> getFilterExpressions() {
    return filterExpressions;
  }

  /**
   * An enumeration that specifies the function to be executed on the filter expression for each
   * group of rows to obtain a single row after deduplication.
   */
  public enum FilterFunction {
    MIN, // Minimum value
    MAX, // Maximum Value
    ANY_NULLS_FIRST, // Any value, with null values prioritized
    ANY_NULLS_LAST // Any value, with non-null values prioritized
  }

  /**
   * A class to combine an {@link Expression} and a {@link FilterFunction} into a single object.
   */
  public static class FilterExpression {

    private final Expression expression;
    private final FilterFunction filterFunction;

    /**
     * Creates a new {@link FilterExpression} using the specified {@link Expression} and {@link
     * FilterFunction}.
     *
     * @param expression the expression using which filtering of duplicate records is to be
     *     performed
     * @param filterFunction the function to be applied on the duplicate records to select a
     *     single record
     */
    public FilterExpression(Expression expression, FilterFunction filterFunction) {
      this.expression = expression;
      this.filterFunction = filterFunction;
    }

    /**
     * @return the expression used for filtering duplicate records
     */
    public Expression getExpression() {
      return expression;
    }

    /**
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
   * Builds a DeduplicateAggregationDefinition using fields to dedup on, fields to select, and
   * fields to use to filter the first record to be output when removing duplicates. The fields to
   * dedup on and the fields to select must be specified.
   */
  public static class Builder {

    List<FilterExpression> filterExpressions;
    private List<Expression> groupByExpressions;
    private Map<String, Expression> selectExpressions;

    public Builder() {
      groupByExpressions = Collections.emptyList();
      selectExpressions = Collections.emptyMap();
      filterExpressions = new ArrayList<>();
    }

    /**
     * Sets the list of expressions to use to group records together with the same value so that
     * only one record from the group is extracted. Any existing dedup expression list is
     * overwritten.
     *
     * @param dedupExpressions list of {@link Expression}s to deduplicate on.
     * @return a {@link Builder} with the currently built {@link DeduplicateAggregationDefinition}.
     */
    public Builder dedupOn(List<Expression> dedupExpressions) {
      groupByExpressions = dedupExpressions;
      return this;
    }

    /**
     * Sets the expressions to use to group records together with the same value so that only one
     * record from the group is extracted. Any existing dedup expression list is overwritten.
     *
     * @param dedupExpressions {@link Expression}s to deduplicate on.
     * @return a {@link Builder} with the currently built {@link DeduplicateAggregationDefinition}.
     */
    public Builder dedupOn(Expression... dedupExpressions) {
      return dedupOn(Arrays.asList(dedupExpressions));
    }

    /**
     * Sets the list of expressions to select to the specified list of expressions. Any existing
     * list of select expressions is overwritten.
     *
     * @param selectExpressions list of {@link Expression}s to select.
     * @return a {@link Builder} with the currently built {@link DeduplicateAggregationDefinition}.
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
     * @return a {@link Builder} with the currently built {@link DeduplicateAggregationDefinition}.
     */
    public Builder select(String key, Expression expression) {
      this.selectExpressions.put(key, expression);
      return this;
    }

    /**
     * Adds the specified expression with the specified filter function to the list of filter
     * expressions. This function is applied on this expression for all the records for which the
     * values of the dedup expressions specified in <code>dedupOn</code> is the same, and the
     * resultant row is extracted by the dedup operation.
     *
     * @param filterExpression an {@link Expression} on which the filter function will be
     *     applied.
     * @param filterFunction a {@link FilterFunction} to be applied to the filter expression.
     * @return a {@link Builder} with the currently built {@link DeduplicateAggregationDefinition}.
     */
    public Builder filterDuplicatesBy(Expression filterExpression, FilterFunction filterFunction) {
      this.filterExpressions.add(new FilterExpression(filterExpression, filterFunction));
      return this;
    }

    /**
     * Sets the list of filter expressions to the specified list of filter functions and filter
     * expressions. The filter function is applied to the respective filter expression for all the
     * records for which the values of the dedup expressions specified in <code>dedupOn</code> is
     * the same, and the resultant row is extracted by the dedup operation.
     *
     * @param filterExpressions a {@link List} of {@link FilterExpression}.
     * @return a {@link Builder} with the currently built {@link DeduplicateAggregationDefinition}.
     */
    public Builder filterDuplicatesBy(List<FilterExpression> filterExpressions) {
      this.filterExpressions = filterExpressions;
      return this;
    }

    /**
     * Builds a DeduplicateAggregationDefinition.
     *
     * @return The DeduplicateAggregationDefinition object.
     * @throws IllegalStateException in case if any of the required fields aren't specified.
     */
    public DeduplicateAggregationDefinition build() {
      if (selectExpressions.isEmpty()) {
        throw new IllegalStateException(
            "Can't build a DeduplicateAggregationDefinition without select fields");
      } else if (groupByExpressions.isEmpty()) {
        throw new IllegalStateException(
            "Can't build a DeduplicateAggregationDefinition without fields to dedup on");
      }

      return new DeduplicateAggregationDefinition(groupByExpressions, selectExpressions,
          filterExpressions);
    }
  }
}
