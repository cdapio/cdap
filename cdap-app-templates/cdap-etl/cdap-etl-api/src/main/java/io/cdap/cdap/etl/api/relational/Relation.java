/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.WindowAggregationDefinition;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * This class defines a relation that can be transformed in a declarative way using relational
 * algebra calls and expressions. It does not provide row-by-row access, but rather a set of
 * transformation calls that will be delegated to the underlying engine.
 */
public interface Relation {

  /**
   * @return if this relation is valid. If any operation requested is not supported, it will return
   *     an invalid relation.
   * @see #getValidationError() on operation problem details
   */
  boolean isValid();

  /**
   * @return validation error if relation is not valid
   * @see #isValid()
   */
  String getValidationError();

  /**
   * Allows to add or replace column for a relation. This operation does not change number of rows.
   *
   * @param column name of the column to add / replace
   * @param value value to set to the column
   * @return a new relation with a column added or replaced
   */
  Relation setColumn(String column, Expression value);

  /**
   * Allows to drop existing column on a relation. This operation does not change number of rows.
   *
   * @param column name of the column to drop
   * @return a new relation that does not have the column
   */
  Relation dropColumn(String column);

  /**
   * Allows to completely replace set of column with a new one. This operation does not change
   * number of rows.
   *
   * @param columns map of column names to value expressions to form new column set
   * @return a new relation with required columns
   */
  Relation select(Map<String, Expression> columns);

  /**
   * Allows to filter relation rows based on a boolean expression
   *
   * @param filter boolean expression to use as a filter
   * @return a new relation with same set of columns, but only rows where filter value is true
   */
  Relation filter(Expression filter);

  /**
   * Allows to perform a group by based on an aggregation definition.
   *
   * @param aggregationDefinition specifies the details for the group by operation.
   * @return a new relation after the grouping is performed.
   */
  default Relation groupBy(GroupByAggregationDefinition aggregationDefinition) {
    return new InvalidRelation("GroupBy is unsupported");
  }

  /**
   * Allows to perform a window operation based on an aggregation definition.
   *
   * @param aggregationDefinition specifies the details for the window aggregation operation.
   * @return a new relation after the window operation is performed.
   */
  default Relation window(WindowAggregationDefinition aggregationDefinition) {
    return new InvalidRelation("WindowAggregation is unsupported");
  }

  /**
   * Allows to perform a deduplicate operation based on an aggregation definition.
   *
   * @param aggregationDefinition specifies the details for the deduplicate operation.
   * @return a new relation after deduplication of rows.
   */
  default Relation deduplicate(DeduplicateAggregationDefinition aggregationDefinition) {
    return new InvalidRelation("Deduplicate is unsupported");
  }

  /**
   * Get the {@link Schema} of the relation if available, null otherwise.
   *
   * @return the schema of the relation.
   */
  @Nullable
  default Schema getSchema() {
    return null;
  }

  /**
   * Adds schema information to the relation. This can be used for validation purposes.
   *
   * @param schema The schema.
   * @return A new relation with the schema added.
   */
  default Relation addSchema(@Nullable Schema schema) {
    return new InvalidRelation("This relation does not support adding Schema information");
  }
}
