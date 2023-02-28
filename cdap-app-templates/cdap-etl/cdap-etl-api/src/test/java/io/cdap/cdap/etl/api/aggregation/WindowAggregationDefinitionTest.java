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

import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.ExpressionFactoryType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link WindowAggregationDefinition} and related builders.
 */
public class WindowAggregationDefinitionTest {

  @Test
  public void testDefinitionBuild() {
    List<Expression> partitionExpressions = new ArrayList<>();
    Map<String, Expression> aggregateExpressions = new HashMap<>();
    List<WindowAggregationDefinition.OrderByExpression> orderByExpressions = new ArrayList<>();
    Map<String, Expression> selectExpressions = new HashMap<>();
    WindowAggregationDefinition.WindowFrameType windowFrameType = WindowAggregationDefinition.WindowFrameType.ROW;
    ExpressionFactory<String> factory = new ExpressionFactory<String>() {
      @Override
      public ExpressionFactoryType<String> getType() {
        return null;
      }

      @Override
      public Set<Capability> getCapabilities() {
        return null;
      }

      @Override
      public Expression compile(String expression) {
        return new Expression() {
          @Override
          public boolean isValid() {
            return true;
          }

          @Override
          public String getValidationError() {
            return null;
          }
        };
      }
    };
    Expression ageExpression = factory.compile("age");

    partitionExpressions.add(factory.compile("firstName"));
    partitionExpressions.add(factory.compile("managerId"));
    aggregateExpressions.put("id", factory.compile("Rank()"));
    selectExpressions.put("employeeId", factory.compile("employeeId"));
    selectExpressions.put("firstName", factory.compile("firstName"));
    selectExpressions.put("lastName", factory.compile("lastName"));
    orderByExpressions.add(new WindowAggregationDefinition.OrderByExpression(factory.compile("age"),
      WindowAggregationDefinition.OrderBy.ASCENDING));
    WindowAggregationDefinition definition = WindowAggregationDefinition.builder()
      .partition(partitionExpressions).aggregate(aggregateExpressions).select(selectExpressions)
      .orderBy(orderByExpressions).windowFrameType(windowFrameType).build();

    Assert.assertEquals(partitionExpressions, definition.getPartitionExpressions());
    Assert.assertEquals(aggregateExpressions, definition.getAggregateExpressions());
    Assert.assertEquals(selectExpressions, definition.getSelectExpressions());
    Assert.assertEquals(orderByExpressions, definition.getOrderByExpressions());
    Assert.assertEquals(windowFrameType, definition.getWindowFrameType());

    try {
      WindowAggregationDefinition invalidDefinition = WindowAggregationDefinition.builder()
        .partition(partitionExpressions).aggregate(aggregateExpressions).orderBy(orderByExpressions)
        .windowFrameType(windowFrameType).build();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException ignored) {
      // expected
    }

    try {
      WindowAggregationDefinition invalidSelectDefinition = WindowAggregationDefinition.builder()
        .select(selectExpressions).build();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException ignored) {
      // expected
    }
  }
}
