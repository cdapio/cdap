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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.sql.engine.relation.SparkSQLExpression;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * An implementation of {@link Relation} which would run SQL statement in SPARK SQL.
 */
public class SparkSQLRelation implements Relation {

  private static final SparkSQLExpressionFactory factory = new SparkSQLExpressionFactory();
  private String datasetName;
  private List<String> columns;
  private String sqlStatement;
  private final Schema schema;

  // SQL KEY WORDS
  private static final String SELECT = "SELECT";
  private static final String FROM = "FROM";
  private static final String WHERE = "WHERE";
  private static final String AS = " AS ";
  private static final String COMMA = " , ";

  public SparkSQLRelation(String datasetName, List<String> columns) {
    this(datasetName, columns, null, null);
  }

  public SparkSQLRelation(String datasetName, List<String> columns, Schema schema) {
    this(datasetName, columns, null, schema);
  }

  SparkSQLRelation(String datasetName, List<String> columns, @Nullable String sqlStatement,
                   @Nullable Schema schema) {
    this.datasetName = datasetName;
    this.columns = columns;
    this.sqlStatement = sqlStatement;
    this.schema = schema;
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public String getValidationError() {
    return null;
  }

  @Override
  public Relation setColumn(String column, Expression value) {

    if (!SparkSQLExpression.supportsExpression(value)) {
      return new InvalidRelation("Unsupported or invalid expression type : " + value);
    }

    Map<String, Expression> columnExpMap = generateColumnExpMap(this.columns);
    columnExpMap.put(column, value);
    List<String> columnList = columnExpMap.keySet().stream().collect(Collectors.toList());
    return new SparkSQLRelation(this.datasetName, columnList, generateSelectQuery(null, columnExpMap), schema);
  }

  @Override
  public Relation dropColumn(String column) {

    if (!this.columns.contains(column)) {
      return new InvalidRelation("Trying to remove non existing column in Relation: " + column);
    }
    this.columns.remove(column);
    return new SparkSQLRelation(this.datasetName, columns, generateSelectQuery(null), schema);
  }

  @Override
  public Relation select(Map<String, Expression> columnExpMap) {
    if (!SparkSQLExpression.supportsExpressions(columnExpMap.values())) {
      return new InvalidRelation("One or more Unsupported or invalid expression type  in the list : "
                                   + columnExpMap.values());
    }
    List<String> columnList = columnExpMap.keySet().stream().collect(Collectors.toList());
    return new SparkSQLRelation(this.datasetName, columnList, generateSelectQuery(null, columnExpMap), schema);
  }

  @Override
  public Relation filter(Expression filter) {
    // Get filter conditions
    String filterCondition = filter != null ? ((SparkSQLExpression) filter).extract() : null;
    return new SparkSQLRelation(this.datasetName, columns, generateSelectQuery(filterCondition), schema);
  }

  public String getSqlStatement() {
    return sqlStatement;
  }

  /**
   * Get the {@link Schema} of the relation if available, null otherwise.
   *
   * @return the schema of the relation.
   */
  @Nullable
  public Schema getSchema() {
    return schema;
  }

  @VisibleForTesting
  public List<String> getColumns() {
    return columns;
  }

  public String getDatasetName() { return datasetName; }

  private String generateSelectQuery(String filterCondition) {
    return generateSelectQuery(filterCondition, generateColumnExpMap(this.columns));
  }

  private String generateSelectQuery(String filterCondition, Map<String, Expression> columnExpMap) {
    StringBuilder queryBuilder = new StringBuilder(
      String.format("%s %s %s (%s)%s%s",
                    SELECT,
                    getColumnAliasCSV(columnExpMap),
                    FROM,
                    getDataSetAlias(),
                    AS,
                    this.datasetName)
    );

    if (filterCondition != null && !filterCondition.isEmpty()) {
      queryBuilder.append(String.format(" %s %s", WHERE, filterCondition));
    }

    return queryBuilder.toString();
  }

  private String getDataSetAlias() {
    return this.getSqlStatement() != null ? this.getSqlStatement() : this.getDatasetName();
  }

  private Map<String, Expression> generateColumnExpMap(List<String> columns) {
    Map<String, Expression> columnExpMap = new LinkedHashMap<>();
    columns.forEach((colName)-> columnExpMap.put(colName, factory.compile(colName)));
    return columnExpMap;
  }

  private String getColumnAliasCSV(Map<String, Expression> columnExpMap) {
    return columnExpMap.entrySet()
      .stream()
      .map(e -> ((SparkSQLExpression) e.getValue()).extract() + AS + e.getKey())
      .collect(Collectors.joining(COMMA));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SparkSQLRelation)) {
      return false;
    }
    SparkSQLRelation that = (SparkSQLRelation) o;
    return Objects.equals(sqlStatement, that.sqlStatement)
      && Objects.equals(datasetName, that.datasetName)
      && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sqlStatement, datasetName, columns);
  }
}
