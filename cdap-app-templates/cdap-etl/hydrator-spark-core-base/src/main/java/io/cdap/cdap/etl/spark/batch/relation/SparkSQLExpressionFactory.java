/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch.relation;

import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.CoreExpressionCapabilities;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.ExpressionFactoryType;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import io.cdap.cdap.etl.api.sql.engine.relation.SparkSQLExpression;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SparkSQLExpressionFactory implements ExpressionFactory<String> {
  private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(
    new HashSet<Capability>() {{
      add(StringExpressionFactoryType.SQL);
      add(CoreExpressionCapabilities.CAN_GET_QUALIFIED_DATASET_NAME);
      add(CoreExpressionCapabilities.CAN_GET_QUALIFIED_COLUMN_NAME);
      add(CoreExpressionCapabilities.CAN_SET_DATASET_ALIAS);
    }});

  @Override
  public ExpressionFactoryType<String> getType() {
    return StringExpressionFactoryType.SQL;
  }

  @Override
  public Set<Capability> getCapabilities() {
    return CAPABILITIES;
  }

  @Override
  public Expression compile(String expression) {
    return new SparkSQLExpression(expression);
  }
}
