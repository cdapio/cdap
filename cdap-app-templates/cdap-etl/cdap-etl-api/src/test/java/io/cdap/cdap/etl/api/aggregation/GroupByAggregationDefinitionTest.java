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
 * Tests for {@link GroupByAggregationDefinition} and related builders.
 */
public class GroupByAggregationDefinitionTest {
  @Test
  public void testDefinitionBuild() {
    List<Expression> groupByExpressions = new ArrayList<>();
    Map<String, Expression> selectExpressions = new HashMap<>();
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

    groupByExpressions.add(factory.compile("department"));
    groupByExpressions.add(factory.compile("managerId"));
    selectExpressions.put("num_employees", factory.compile("count(*)"));
    selectExpressions.put("total_salaries", factory.compile("sum(salary)"));

    GroupByAggregationDefinition definition = GroupByAggregationDefinition.builder()
      .groupBy(groupByExpressions)
      .select(selectExpressions)
      .build();

    Assert.assertEquals(groupByExpressions, definition.getGroupByExpressions());
    Assert.assertEquals(selectExpressions, definition.getSelectExpressions());

    try {
      GroupByAggregationDefinition invalidDefinition = GroupByAggregationDefinition.builder()
        .groupBy(groupByExpressions)
        .build();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException ignored) {
      // expected
    }
  }
}
