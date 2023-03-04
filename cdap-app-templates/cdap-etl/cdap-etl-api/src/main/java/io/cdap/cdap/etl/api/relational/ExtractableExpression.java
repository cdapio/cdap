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

import javax.annotation.Nullable;

/**
 * Expression that can be used to derive a scalar value. This value can then be used to build a new
 * valid expression using {@link ExpressionFactory#compile} Note that relations, engine and
 * expression factories must come from the same {@link RelationalTranformContext} and are valid only
 * for the time of the {@link RelationalTransform#transform} call.
 *
 * @param <T> type of the scalar value contained in this expression
 */
public interface ExtractableExpression<T> extends Expression {

  /**
   * Get the value contained in this expression. This value is safe to use when building expressions
   * using {@link ExpressionFactory#compile} The return value for this method will be null if the
   * expression is invalid.
   *
   * @return scalar value contained in this expression, or null if the expression is invalid.
   * @throws UnsupportedOperationException if the expression is invalid.
   */
  @Nullable
  T extract() throws InvalidExtractableExpressionException;

}
