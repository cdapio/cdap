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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.etl.api.engine.sql.BatchSQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.capability.DefaultPullCapability;
import io.cdap.cdap.etl.api.engine.sql.capability.DefaultPushCapability;
import io.cdap.cdap.etl.api.engine.sql.capability.PullCapability;
import io.cdap.cdap.etl.api.engine.sql.capability.PushCapability;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetConsumer;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetProducer;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLRelationDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLTransformDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLTransformRequest;
import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.DelegatingMultiRelation;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.LinearRelationalTransformCapabilities;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLDataset;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLExpressionFactory;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLPullProducer;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLPushConsumer;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLRelation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Spark SQLEngine implementation, where compatible ANSI SQL in the given transformation will
 * run in Spark SQL over a Dataset.
 */
public class SparkSQLEngine extends BatchSQLEngine<Object, Object, Object, Object>
  implements Engine {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSQLEngine.class);

  @Override
  public SQLDatasetConsumer getConsumer(SQLPushRequest pushRequest, PushCapability capability) {
    return new SparkSQLPushConsumer(pushRequest);
  }

  @Override
  public SQLDatasetProducer getProducer(SQLPullRequest pullRequest, PullCapability capability) {
    return new SparkSQLPullProducer(pullRequest);
  }

  @Override
  public boolean supportsRelationalTranform() {
    return true;
  }

  @Override
  public Relation getRelation(SQLRelationDefinition relationDefinition) {
   List<String> columns = relationDefinition.getSchema().getFields().stream().map(f -> f.getName())
     .collect(Collectors.toList());

   String datasetName = getNormalizedDatasetName(relationDefinition.getDatasetName());
    return new SparkSQLRelation(datasetName, columns);
  }

  @Override
  public SQLDataset transform(SQLTransformRequest context) throws SQLEngineException {
    SparkSQLRelation sparkSQLRelation = getSparkSQLRelationFromContext(context);

    List<SparkSQLDataset> sparkDatasets = context.getInputDataSets()
      .values()
      .stream()
      .map(ds -> (SparkSQLDataset) ds)
      .collect(Collectors.toList());

    return executeSQL(sparkSQLRelation.getSqlStatement(), sparkDatasets, context);
  }

  @Override
  public Set<PushCapability> getPushCapabilities() {
    HashSet<PushCapability> pushCapabilities = new HashSet<>();
    pushCapabilities.add(DefaultPushCapability.SPARK_RDD_PUSH);
    return Collections.unmodifiableSet(pushCapabilities);
  }

  @Override
  public Set<PullCapability> getPullCapabilities() {
    HashSet<PullCapability> pullCapability = new HashSet<>();
    pullCapability.add(DefaultPullCapability.SPARK_RDD_PULL);
    return Collections.unmodifiableSet(pullCapability);
  }

  @Override
  public SQLPushDataset getPushProvider(SQLPushRequest pushRequest) throws SQLEngineException {
    return null;
  }

  @Override
  public SQLPullDataset getPullProvider(SQLPullRequest pullRequest) throws SQLEngineException {
    return null;
  }

  @Override
  public boolean canTransform(SQLTransformDefinition transformDefinition) {
    return true;
  }


  @Override
  public boolean exists(String datasetName) throws SQLEngineException {
    return false;
  }

  @Override
  public boolean canJoin(SQLJoinDefinition joinDefinition) {
    return false;
  }

  @Override
  public SQLDataset join(SQLJoinRequest joinRequest) throws SQLEngineException {
    return null;
  }

  @Override
  public void cleanup(String datasetName) throws SQLEngineException { }

  @Override
  public Engine getRelationalEngine() {
    return this;
  }

  @Override
  public Set<Capability> getCapabilities() {
    Set<Capability> capabilities = new HashSet<>();
    capabilities.add(StringExpressionFactoryType.SQL);
    capabilities.add(LinearRelationalTransformCapabilities.CAN_HANDLE_MULTIPLE_INPUTS);
    return Collections.unmodifiableSet(capabilities);
  }

  @Override
  public List<ExpressionFactory<?>> getExpressionFactories() {
    return Collections.singletonList(new SparkSQLExpressionFactory());
  }

  private SparkSQLDataset executeSQL(String sqlStatement,
                                     List<SparkSQLDataset> sparkDatasets,
                                     SQLTransformRequest context) {
    Dataset<Row> combinedDs = null;

    // Combine all input datasets to execute this statement.
    for (SparkSQLDataset sparkDataset : sparkDatasets) {
      Dataset<Row> ds = sparkDataset.getDs();
      combinedDs = combinedDs == null ? ds : combinedDs.union(ds);
    }

    // Create temporary view for table
    LOG.debug("Creating temp view for stage : {}", context.getOutputDatasetName());
    combinedDs.createOrReplaceTempView("relational_transform_stage");

    // Execute filter statement for this input stage.
    SparkSession sparkSession = combinedDs.sparkSession();
    LOG.info("Executing SQL in spark : {}", sqlStatement);
    Dataset<Row> result = sparkSession.sql(sqlStatement);

    return new SparkSQLDataset(result, result.count(), context.getOutputDatasetName(), context.getOutputSchema());
  }

  // Spark SQL View Naming syntax
  // Replace all non Alphanumeric chars with "_"
  private String getNormalizedDatasetName(String datasetName) {
    return datasetName.replaceAll("[^A-Za-z0-9]", "_");
  }

  /**
   * Find an instance of {@link SparkSQLRelation} from a {@link SQLTransformRequest}'s Output Relation.
   * @param transformRequest the SQL Trasnsformation request
   * @return a SparkSQLRelation instance from this request
   * @thrpws {@link IllegalArgumentException} if the supplied {@link SQLTransformRequest} doesn't contain any output
   * relation of type {@link SparkSQLRelation}.
   */
  private SparkSQLRelation getSparkSQLRelationFromContext(SQLTransformRequest transformRequest) {
    Relation outputRelation = transformRequest.getOutputRelation();
    if (outputRelation instanceof SparkSQLRelation) {
      return (SparkSQLRelation) outputRelation;
    }

    if (outputRelation instanceof DelegatingMultiRelation) {
      List<Relation> delegates = ((DelegatingMultiRelation) outputRelation).getDelegates();

      for (Relation delegate : delegates) {
        if (delegate instanceof SparkSQLRelation) {
          return (SparkSQLRelation) delegate;
        }
      }

      throw new IllegalArgumentException("DelegatingMultiRelation did not contain any SparkSQLRelation instances.");
    }

    throw new IllegalArgumentException(
      String.format("Invalid Relation type: %s. Must be either SparkSQLRelation or DelegatingMultiRelation.",
                    outputRelation.getClass().getName()));
  }
}
