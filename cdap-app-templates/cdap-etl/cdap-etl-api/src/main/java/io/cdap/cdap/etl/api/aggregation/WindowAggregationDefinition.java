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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Specifies how a window aggregation operation should be executed.
 */
public class WindowAggregationDefinition {
  private List<Expression> partitionExpressions;
  private Map<String, Expression> selectExpressions;
  private List<OrderByExpression> orderByExpressions;
  private WindowFrameType windowFrameType;

  private WindowAggregationDefinition(Map<String, Expression> selectExpressions,
                                      List<Expression> partitionExpressions,
                                      List<OrderByExpression> orderByExpressions,
                                      WindowFrameType windowFrameType) {
    this.selectExpressions = selectExpressions;
    this.partitionExpressions = partitionExpressions;
    this.orderByExpressions = orderByExpressions;
    this.windowFrameType = windowFrameType;
  }

  /**
   * @return A builder to create a WindowAggregationDefinition.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return select expressions of WindowAggregationDefinition.
   */
  public Map<String, Expression> getSelectExpressions() {
    return selectExpressions;
  }

  /**
   * @return window frame type value of WindowAggregationDefinition.
   */
  public WindowFrameType getWindowFrameType() {
    return windowFrameType;
  }

  /**
   * @return order by expressions of WindowAggregationDefinition.
   */
  public List<OrderByExpression> getOrderByExpressions() {
    return orderByExpressions;
  }

  /**
   * @return partition expressions of WindowAggregationDefinition.
   */
  public List<Expression> getPartitionExpressions() {
    return partitionExpressions;
  }

  /**
   * An enumeration that specifies the type of windowFrame
   */
  public enum WindowFrameType {
    ROW,
    RANGE,
    NONE
  }

  /**
   * An enumeration that specifies the type of order by
   */
  public enum OrderBy {
    ASCENDING,
    DESCENDING
  }

  /**
   * A class to combine an {@link Expression} and a {@link OrderBy} into a single object.
   */
  public static class OrderByExpression {
    private final Expression expression;
    private final OrderBy orderBy;

    /**
     * Creates a new {@link OrderByExpression} using the specified {@link Expression} and {@link orderBy}.
     *
     * @param expression the expression using which ordering of records is to be performed
     * @param orderBy    the function to be applied on the records to choose order
     */
    public OrderByExpression(Expression expression, OrderBy orderBy) {
      this.expression = expression;
      this.orderBy = orderBy;
    }

    /**
     * @return the expression used for ordering records
     */
    public Expression getExpression() {
      return expression;
    }

    /**
     * @return the function applied to order ascending or descending records
     */
    public OrderBy getOrderBy() {
      return orderBy;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OrderByExpression that = (OrderByExpression) o;
      return Objects.equals(getExpression(), that.getExpression()) && getOrderBy() == that.getOrderBy();
    }

    @Override
    public int hashCode() {
      return Objects.hash(getExpression(), getOrderBy());
    }
  }

  /**
   * Builds a WindowAggregationDefinition using fields to partition  and fields to select.
   * The fields to select must be specified.
   */
  public static class Builder {
    private List<Expression> partitionExpressions;
    private Map<String, Expression> selectExpressions;
    private List<OrderByExpression> orderByExpressions;
    private WindowFrameType windowFrameType;

    private Builder() {
      partitionExpressions = new ArrayList();
      selectExpressions = new HashMap();
      orderByExpressions = new ArrayList();
      windowFrameType = WindowFrameType.NONE;
    }

    /**
     * Sets the list of expressions to perform partition to the specified list.
     *
     * @param partitionExpressions list of {@link Expression}s.
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder partition(List<Expression> partitionExpressions) {
      this.partitionExpressions = partitionExpressions;
      return this;
    }

    /**
     * Sets the list of expressions to perform order by to the specified list.
     *
     * @param orderByExpressions list of {@link Expression}s.
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder orderBy(List<OrderByExpression> orderByExpressions) {
      this.orderByExpressions = orderByExpressions;
      return this;
    }

    /**
     * Sets the list of expressions to perform partition to the specified expressions.
     * Any existing partition expression list is overwritten.
     *
     * @param partitionExpressions {@link Expression}s.
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder partition(Expression... partitionExpressions) {
      return partition(Arrays.asList(partitionExpressions));
    }

    /**
     * Any window frame type value is overwritten.
     *
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder windowFrameType(WindowFrameType windowFrameType) {
      this.windowFrameType = windowFrameType;
      return this;
    }

    /**
     * Sets the list of expressions to perform order by to the specified expressions.
     * Any existing order by expression list is overwritten.
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder orderBy(Expression expression, OrderBy orderBy) {
      this.orderByExpressions.add(new OrderByExpression(expression, orderBy));
      return this;
    }

    /**
     * Sets the list of expressions to select to the specified expressions.
     * Any existing list of select expressions is overwritten.
     *
     * @param selectExpressions to use
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder select(Map<String, Expression> selectExpressions) {
      this.selectExpressions = selectExpressions;
      return this;
    }

    /**
     * Builds a WindowAggregationDefinition.
     *
     * @return A WindowAggregationDefinition object.
     * @throws IllegalStateException in case the fields to select are not specified.
     */
    public WindowAggregationDefinition build() {
      if (selectExpressions.isEmpty()) {
        throw new IllegalStateException("Can't build a WindowAggregationDefinition without select fields");
      } else if (partitionExpressions.isEmpty()) {
        throw new IllegalStateException("Can't build a WindowAggregationDefinition without fields to partition");
      }
      return new WindowAggregationDefinition(selectExpressions, partitionExpressions, orderByExpressions,
                                             windowFrameType);
    }
  }
}
