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
import javax.annotation.Nullable;

/**
 * Specifies how a window aggregation operation should be executed.
 */
public class WindowAggregationDefinition {

  private List<Expression> partitionExpressions;
  private Map<String, Expression> aggregateExpressions;
  private Map<String, Expression> selectExpressions;
  private List<OrderByExpression> orderByExpressions;
  private WindowFrameType windowFrameType;
  private boolean unboundedPreceding;
  private boolean unboundedFollowing;
  private String preceding;
  private String following;

  private WindowAggregationDefinition(Map<String, Expression> selectExpressions,
      List<Expression> partitionExpressions,
      Map<String, Expression> aggregateExpressions,
      @Nullable List<OrderByExpression> orderByExpressions,
      WindowFrameType windowFrameType,
      @Nullable boolean unboundedPreceding,
      @Nullable boolean unboundedFollowing,
      @Nullable String preceding,
      @Nullable String following) {
    this.selectExpressions = selectExpressions;
    this.partitionExpressions = partitionExpressions;
    this.aggregateExpressions = aggregateExpressions;
    this.orderByExpressions = orderByExpressions;
    this.windowFrameType = windowFrameType;
    this.unboundedPreceding = unboundedPreceding;
    this.unboundedFollowing = unboundedFollowing;
    this.preceding = preceding;
    this.following = following;
  }

  /**
   * @return A builder to create a WindowAggregationDefinition.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return value of unbounded preceding field of WindowAggregationDefinition.
   */
  @Nullable
  public boolean getUnboundedPreceding() {
    return unboundedPreceding;
  }

  /**
   * @return value of UnboundedFollowing field of WindowAggregationDefinition.
   */
  @Nullable
  public boolean getUnboundedFollowing() {
    return unboundedFollowing;
  }

  /**
   * @return value of preceding field of WindowAggregationDefinition.
   */
  @Nullable
  public String getPreceding() {
    return preceding;
  }

  /**
   * @return value of following field of WindowAggregationDefinition.
   */
  @Nullable
  public String getFollowing() {
    return following;
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
  @Nullable
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
   * @return aggregate expressions of WindowAggregationDefinition.
   */
  public Map<String, Expression> getAggregateExpressions() {
    return aggregateExpressions;
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
     * Creates a new {@link OrderByExpression} using the specified {@link Expression} and {@link
     * orderBy}.
     *
     * @param expression the expression using which ordering of records is to be performed
     * @param orderBy the function to be applied on the records to choose order
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
    @Nullable
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
      return Objects.equals(getExpression(), that.getExpression())
          && getOrderBy() == that.getOrderBy();
    }

    @Override
    public int hashCode() {
      return Objects.hash(getExpression(), getOrderBy());
    }
  }

  /**
   * Builds a WindowAggregationDefinition using fields to partition  and fields to select. The
   * fields to select must be specified.
   */
  public static class Builder {

    private List<Expression> partitionExpressions;
    private Map<String, Expression> aggregateExpressions;
    private Map<String, Expression> selectExpressions;
    private List<OrderByExpression> orderByExpressions;
    private WindowFrameType windowFrameType;
    private boolean unboundedPreceding;
    private boolean unboundedFollowing;
    private String preceding;
    private String following;

    private Builder() {
      partitionExpressions = new ArrayList();
      aggregateExpressions = new HashMap();
      selectExpressions = new HashMap();
      orderByExpressions = new ArrayList();
      windowFrameType = WindowFrameType.NONE;
    }

    /**
     * Sets unbounded Preceding if window frame type is row or range
     *
     * @param unboundedPreceding value
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder unboundedPreceding(@Nullable boolean unboundedPreceding) {
      this.unboundedPreceding = unboundedPreceding;
      return this;
    }

    /**
     * Sets unbounded Preceding if window frame type is row or range
     *
     * @param unboundedFollowing value
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder unboundedFollowing(@Nullable boolean unboundedFollowing) {
      this.unboundedFollowing = unboundedFollowing;
      return this;
    }

    /**
     * Sets the preceding if window frame type is row or range .
     *
     * @param preceding value
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder preceding(@Nullable String preceding) {
      this.preceding = preceding;
      return this;
    }

    /**
     * Sets the following if window frame type is row or range .
     *
     * @param following value
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder following(@Nullable String following) {
      this.following = following;
      return this;
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
     * Sets the list of expressions to perform aggregate to the specified list.
     *
     * @param aggregateExpressions list of {@link Expression}s.
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder aggregate(Map<String, Expression> aggregateExpressions) {
      this.aggregateExpressions = aggregateExpressions;
      return this;
    }

    /**
     * Sets the list of expressions to perform order by to the specified list.
     *
     * @param orderByExpressions list of {@link Expression}s.
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder orderBy(@Nullable List<OrderByExpression> orderByExpressions) {
      this.orderByExpressions = orderByExpressions;
      return this;
    }

    /**
     * Sets the list of expressions to perform partition to the specified expressions. Any existing
     * partition expression list is overwritten.
     *
     * @param partitionExpressions {@link Expression}s.
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder partition(Expression... partitionExpressions) {
      return partition(Arrays.asList(partitionExpressions));
    }

    /**
     * Sets the aggregate function to perform aggregation to the specified expressions. Any existing
     * aggregation expression key is overwritten.
     *
     * @param key {@link String} to put in aggregate expressions
     * @param expression {@link Expression} aggregate function of key in aggregate expressions
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder aggregate(String key, Expression expression) {
      this.aggregateExpressions.put(key, expression);
      return this;
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
     * Sets the list of expressions to perform order by to the specified expressions. Any existing
     * order by expression list is overwritten.
     *
     * @return a {@link Builder} with the currently built {@link WindowAggregationDefinition}.
     */
    public Builder orderBy(Expression expression, OrderBy orderBy) {
      this.orderByExpressions.add(new OrderByExpression(expression, orderBy));
      return this;
    }

    /**
     * Sets the list of expressions to select to the specified expressions. Any existing list of
     * select expressions is overwritten.
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
        throw new IllegalStateException(
            "Can't build a WindowAggregationDefinition without select fields");
      } else if (partitionExpressions.isEmpty()) {
        throw new IllegalStateException(
            "Can't build a WindowAggregationDefinition without fields to partition");
      } else if (aggregateExpressions.isEmpty()) {
        throw new IllegalStateException(
            "Can't build a WindowAggregationDefinition without fields to Aggregate");
      }
      return new WindowAggregationDefinition(selectExpressions, partitionExpressions,
          aggregateExpressions,
          orderByExpressions, windowFrameType, unboundedPreceding, unboundedFollowing, preceding,
          following);
    }
  }
}
