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

import io.cdap.cdap.api.data.format.StructuredRecord;
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
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLDataset;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLExpressionFactory;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLPullProducer;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLPushConsumer;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLRelation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
    SparkSQLRelation sparkSQLRelation = (SparkSQLRelation) context.getOutputRelation();

    if (context.getInputDataSets().values().size() > 1) {
      throw new SQLEngineException("Only linear transformation is supported as of now, " +
                                     "the input data contains more than  1 dataset");
    }

    Map<String, SparkSQLDataset> sparkDatasets = context.getInputDataSets().entrySet()
      .stream()
      .collect(Collectors.toMap(
        k -> getNormalizedDatasetName(k.getKey()),
        e -> (SparkSQLDataset) e.getValue()));
    List<String> sqlStatements = sparkSQLRelation.getSqlStatements();
    return executeSQL(sqlStatements, sparkDatasets, context);
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
    return Collections.singleton(StringExpressionFactoryType.SQL);
  }

  @Override
  public List<ExpressionFactory<?>> getExpressionFactories() {
    return Collections.singletonList(new SparkSQLExpressionFactory());
  }

  private SparkSQLDataset executeSQL(List<String> sqlStatements, Map<String, SparkSQLDataset>  sparkDatasets,
                                     SQLTransformRequest context) {
    // Register all datasets as temp view in spark sql
    for (Map.Entry<String, SparkSQLDataset> dsEntry : sparkDatasets.entrySet()) {
      LOG.debug("Creating temp view for dataset : {}", dsEntry.getKey());
      Dataset<Row> ds = dsEntry.getValue().getDs();
      ds.createOrReplaceTempView(dsEntry.getKey());
    }

    SparkSQLDataset sparkSQLDataset = sparkDatasets.values().stream().findFirst().get();
    Dataset<Row> ds = sparkSQLDataset.getDs();
    SparkSession sparkSession = ds.sparkSession();
    for(String sqlStatement: sqlStatements) {
      LOG.info("Executing SQL in spark : {}", sqlStatement);
      ds = sparkSession.sql(sqlStatement);
      ds.createOrReplaceTempView(sparkDatasets.keySet().stream().findFirst().get());
    }
    //TODO Doubt : df.count will trigger the action. ? Expected ?;
    return new SparkSQLDataset(ds, ds.count(), context.getOutputDatasetName(), context.getOutputSchema());
  }

  // Spark SQL View Naming syntax
  // Replace all non Alphanumeric chars with "_"
  private String getNormalizedDatasetName(String datasetName) {
    return datasetName.replaceAll("[^A-Za-z0-9]", "_");
  }
}
