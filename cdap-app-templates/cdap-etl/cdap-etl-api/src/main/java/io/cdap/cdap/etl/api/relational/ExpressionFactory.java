/*
 * Copyright © 2021 Cask Data, Inc.
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

import java.util.Set;

/**
 * An expression factory for the specific language that can compile expressions into
 * language-independent form to be used in {@link Relation} calls.
 *
 * @param <T> java type of expressions accepted. Can be {@link String} in simple cases or
 *     structured objects for complex expressions
 */
public interface ExpressionFactory<T> {

  /**
   * @return factory type that define both expression language and source expression java type
   */
  ExpressionFactoryType<T> getType();

  /**
   * @return set of capabilities. At the very least contains {@link #getType()} capability
   */
  Set<Capability> getCapabilities();

  /**
   * Compiles expression into language-independent form
   *
   * @param expression expression to compile with
   * @return language-independent expression object that can be used in {@link Relation} calls.
   *     Check {@link Expression#isValid()} for any validation errors
   */
  Expression compile(T expression);

  /**
   * Compiles expression into language-independent form taking the input relation into account.
   * The relation may be used for purposes such as expression validation. It is ignored by default.
   *
   * @param expression expression to compile with
   * @param relation input relation on which the expression will operate
   * @return language-independent expression object that can be used in {@link Relation} calls.
   *     Check {@link Expression#isValid()} for any validation errors
   */
  default Expression compile(T expression, Relation relation) {
    return compile(expression);
  }

  /**
   * Provides fully qualified name to be used in expressions for a given dataset. Available with
   * {@link CoreExpressionCapabilities#CAN_GET_QUALIFIED_DATASET_NAME}
   */
  default ExtractableExpression<T> getQualifiedDataSetName(Relation dataSet) {
    return new InvalidExtractableExpression<T>("Method getQualifiedDataSetName not implemented");
  }

  /**
   * Provides fully qualified column name to be used in expressions for a given dataset column.
   * Available with {@link CoreExpressionCapabilities#CAN_GET_QUALIFIED_COLUMN_NAME}
   */
  default ExtractableExpression<T> getQualifiedColumnName(Relation dataSet, String column) {
    return new InvalidExtractableExpression<T>("Method getQualifiedColumnName not implemented");
  }

  /**
   * Allows to set an alias for dataset that can be used for unqualified dataset and column
   * references. Depending on language alias can be case-sensitive or case-insensitive. Available
   * with {@link CoreExpressionCapabilities#CAN_SET_DATASET_ALIAS}
   */
  default Relation setDataSetAlias(Relation dataSet, String alias) {
    return new InvalidRelation("Method setDataSetAlias not implemented.");
  }
}
