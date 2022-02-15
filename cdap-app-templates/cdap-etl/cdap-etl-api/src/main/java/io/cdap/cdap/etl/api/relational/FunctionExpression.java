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

import java.util.Objects;

/**
 * Defines Group By operation based on a supplied Group By Function and a String expression.
 */
public class FunctionExpression {
  private final SQLFunction function;
  private final Expression expression;

  /**
   * Used by the plugin to wrap a compiled expression in a Function.
   * @param function SQL Function to use
   * @param expression Expression to wrap
   */
  public FunctionExpression(SQLFunction function, Expression expression) {
    this.function = function;
    this.expression = expression;
  }

  /**
   * Gets SQL Function in this expression
   * @return function to use.
   */
  public SQLFunction getFunction() {
    return function;
  }

  /**
   * Gets inner expression
   * @return inner expression
   */
  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FunctionExpression that = (FunctionExpression) o;
    return Objects.equals(function, that.function) && Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(function, expression);
  }
}
