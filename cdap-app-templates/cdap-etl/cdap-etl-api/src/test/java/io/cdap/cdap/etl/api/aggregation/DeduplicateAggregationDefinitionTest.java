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
 * Tests for {@link DeduplicateAggregationDefinition} and related builders.
 */
public class DeduplicateAggregationDefinitionTest {
  @Test
  public void testDefinitionBuild() {
    List<Expression> dedupExpressions = new ArrayList<>();
    Map<String, Expression> selectExpressions = new HashMap<>();
    List<DeduplicateAggregationDefinition.FilterExpression> filterExpressions = new ArrayList<>();

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
    filterExpressions.add(new DeduplicateAggregationDefinition.FilterExpression(
      ageExpression, DeduplicateAggregationDefinition.FilterFunction.MIN));

    dedupExpressions.add(factory.compile("firstName"));
    selectExpressions.put("employeeId", factory.compile("employeeId"));
    selectExpressions.put("firstName", factory.compile("firstName"));
    selectExpressions.put("lastName", factory.compile("lastName"));

    DeduplicateAggregationDefinition definition = DeduplicateAggregationDefinition.builder()
      .dedupOn(dedupExpressions)
      .select(selectExpressions)
      .filterDuplicatesBy(ageExpression, DeduplicateAggregationDefinition.FilterFunction.MIN)
      .build();

    Assert.assertEquals(dedupExpressions, definition.getGroupByExpressions());
    Assert.assertEquals(selectExpressions, definition.getSelectExpressions());
    Assert.assertEquals(filterExpressions, definition.getFilterExpressions());

    try {
      DeduplicateAggregationDefinition definitionWithoutSelect = DeduplicateAggregationDefinition.builder()
        .dedupOn(dedupExpressions)
        .filterDuplicatesBy(ageExpression, DeduplicateAggregationDefinition.FilterFunction.MIN)
        .build();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException ignored) {
      // expected
    }

    try {
      DeduplicateAggregationDefinition definitionWithoutDedupExpressions = DeduplicateAggregationDefinition.builder()
        .select(selectExpressions)
        .filterDuplicatesBy(ageExpression, DeduplicateAggregationDefinition.FilterFunction.MIN)
        .build();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException ignored) {
      // expected
    }
  }
}
