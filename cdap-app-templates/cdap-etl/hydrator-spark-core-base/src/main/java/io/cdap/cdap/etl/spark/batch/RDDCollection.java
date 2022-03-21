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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.function.CountingFunction;
import io.cdap.cdap.etl.spark.function.DatasetAggregationAccumulator;
import io.cdap.cdap.etl.spark.function.DatasetAggregationFinalizeFunction;
import io.cdap.cdap.etl.spark.function.DatasetAggregationGetKeyFunction;
import io.cdap.cdap.etl.spark.function.DatasetAggregationReduceFunction;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import io.cdap.cdap.etl.spark.join.JoinCollection;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import io.cdap.cdap.etl.spark.plugin.LiteralsBridge;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import static org.apache.spark.sql.functions.coalesce;

/**
 * Spark2 RDD collection.
 *
 * @param <T> type of object in the collection
 */
public class RDDCollection<T> extends BaseRDDCollection<T> {
  private static final Logger LOG = LoggerFactory.getLogger(RDDCollection.class);
  private static final Encoder KRYO_OBJECT_ENCODER = Encoders.kryo(Object.class);
  private static final Encoder KRYO_TUPLE_ENCODER = Encoders.tuple(
    KRYO_OBJECT_ENCODER, KRYO_OBJECT_ENCODER);
  private static final Encoder JAVA_OBJECT_ENCODER = Encoders.javaSerialization(Object.class);
  private static final Encoder JAVA_TUPLE_ENCODER = Encoders.tuple(
    JAVA_OBJECT_ENCODER, JAVA_OBJECT_ENCODER);

  private final boolean useDatasetAggregation;
  private final boolean useKryoForDatasets;
  private final boolean ignorePartitionsDuringDatasetAggregation;

  public RDDCollection(JavaSparkExecutionContext sec, FunctionCache.Factory functionCacheFactory,
                       JavaSparkContext jsc, SQLContext sqlContext,
                       DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory,
                       JavaRDD<T> rdd) {
    super(sec, functionCacheFactory, jsc, sqlContext, datasetContext, sinkFactory, rdd);
    this.useDatasetAggregation = Boolean.parseBoolean(
      sec.getRuntimeArguments().getOrDefault(Constants.DATASET_AGGREGATE_ENABLED, Boolean.TRUE.toString()));
    this.useKryoForDatasets = Boolean.parseBoolean(
      sec.getRuntimeArguments().getOrDefault(Constants.DATASET_KRYO_ENABLED, Boolean.TRUE.toString()));
    this.ignorePartitionsDuringDatasetAggregation = Boolean.parseBoolean(
      sec.getRuntimeArguments().getOrDefault(Constants.DATASET_AGGREGATE_IGNORE_PARTITIONS, Boolean.TRUE.toString()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> join(JoinRequest joinRequest) {
    Map<String, Dataset> collections = new HashMap<>();
    String stageName = joinRequest.getStageName();
    Function<StructuredRecord, StructuredRecord> recordsInCounter =
      new CountingFunction<>(stageName, sec.getMetrics(), Constants.Metrics.RECORDS_IN, sec.getDataTracer(stageName));
    StructType leftSparkSchema = DataFrames.toDataType(joinRequest.getLeftSchema());
    Dataset<Row> left = toDataset(((JavaRDD<StructuredRecord>) rdd).map(recordsInCounter), leftSparkSchema);
    collections.put(joinRequest.getLeftStage(), left);

    List<Column> leftJoinColumns = joinRequest.getLeftKey().stream()
      .map(left::col)
      .collect(Collectors.toList());

    /*
        This flag keeps track of whether there is at least one required stage in the join.
        This is needed in case there is a join like:

        A (optional), B (required), C (optional), D (required)

        The correct thing to do here is:

        1. A right outer join B as TMP1
        2. TMP1 left outer join C as TMP2
        3. TMP2 inner join D

        Join #1 is a straightforward join between 2 sides.
        Join #2 is a left outer because TMP1 becomes 'required', since it uses required input B.
        Join #3 is an inner join even though it contains 2 optional datasets, because 'B' is still required.
     */
    Integer joinPartitions = joinRequest.getNumPartitions();
    boolean seenRequired = joinRequest.isLeftRequired();
    Dataset<Row> joined = left;
    List<List<Column>> listOfListOfLeftCols = new ArrayList<>();

    for (JoinCollection toJoin : joinRequest.getToJoin()) {
      SparkCollection<StructuredRecord> data = (SparkCollection<StructuredRecord>) toJoin.getData();
      StructType sparkSchema = DataFrames.toDataType(toJoin.getSchema());
      Dataset<Row> right = toDataset(((JavaRDD<StructuredRecord>) data.getUnderlying()).map(recordsInCounter),
                                     sparkSchema);
      collections.put(toJoin.getStage(), right);

      List<Column> rightJoinColumns = toJoin.getKey().stream()
        .map(right::col)
        .collect(Collectors.toList());

      // UUID for salt column name to avoid name collisions
      String saltColumn = UUID.randomUUID().toString();
      if (joinRequest.isDistributionEnabled()) {

        boolean isLeftStageSkewed =
          joinRequest.getLeftStage().equals(joinRequest.getDistribution().getSkewedStageName());

        // Apply salt/explode transformations to each Dataset
        if (isLeftStageSkewed) {
          left = saltDataset(left, saltColumn, joinRequest.getDistribution().getDistributionFactor());
          right = explodeDataset(right, saltColumn, joinRequest.getDistribution().getDistributionFactor());
        } else {
          left = explodeDataset(left, saltColumn, joinRequest.getDistribution().getDistributionFactor());
          right = saltDataset(right, saltColumn, joinRequest.getDistribution().getDistributionFactor());
        }

        // Add the salt column to the join key
        leftJoinColumns.add(left.col(saltColumn));
        rightJoinColumns.add(right.col(saltColumn));

        // Updating other values that will be used later in join
        joined = left;
        sparkSchema = sparkSchema.add(saltColumn, DataTypes.IntegerType, false);
        leftSparkSchema = leftSparkSchema.add(saltColumn, DataTypes.IntegerType, false);
      }

      Column joinOn;
      List<Column> finalLeftJoinColumns = leftJoinColumns; //Making effectively final to use in streams

      if (seenRequired) {
        joinOn = IntStream.range(0, leftJoinColumns.size())
          .mapToObj(i -> eq(finalLeftJoinColumns.get(i), rightJoinColumns.get(i), joinRequest.isNullSafe()))
          .reduce((a, b) -> a.and(b)).get();
      } else {
        // For the case when all joins are outer. Collect left keys at each level (each iteration)
        // coalesce these keys at each level and compare with right
        joinOn = IntStream.range(0, leftJoinColumns.size())
          .mapToObj(i -> {
            collectLeftJoinOnCols(listOfListOfLeftCols, i, finalLeftJoinColumns.get(i));
            return eq(getLeftJoinOnCoalescedColumn(finalLeftJoinColumns.get(i), i, listOfListOfLeftCols),
                      rightJoinColumns.get(i),
                      joinRequest.isNullSafe());
          })
          .reduce((a, b) -> a.and(b)).get();
      }

      String joinType;
      if (seenRequired && toJoin.isRequired()) {
        joinType = "inner";
      } else if (seenRequired && !toJoin.isRequired()) {
        joinType = "leftouter";
      } else if (!seenRequired && toJoin.isRequired()) {
        joinType = "rightouter";
      } else {
        joinType = "outer";
      }
      seenRequired = seenRequired || toJoin.isRequired();

      if (toJoin.isBroadcast()) {
        right = functions.broadcast(right);
      }
      // repartition on the join keys with the number of partitions specified in the join request.
      // since they are partitioned on the same thing, spark will not repartition during the join,
      // which allows us to use a different number of partitions per joiner instead of using the global
      // spark.sql.shuffle.partitions setting in the spark conf.
      // Note that it does not work with Spark 2.3+ as they changed partitioning column set in
      // https://github.com/apache/spark/pull/19937. Now we ignore user setting unless
      // we are forced to with spark.cdap.pipeline.aggregate.dataset.partitions.ignore = false
      if (!ignorePartitionsDuringDatasetAggregation && joinPartitions != null && !toJoin.isBroadcast()) {
        List<String> rightKeys = new ArrayList<>(toJoin.getKey());
        List<String> leftKeys = new ArrayList<>(joinRequest.getLeftKey());

        // If distribution is enabled we need to add it to the partition keys to ensure we end up with the desired
        // number of partitions
        if (joinRequest.isDistributionEnabled()) {
          rightKeys.add(saltColumn);
          leftKeys.add(saltColumn);
        }
        right = partitionOnKey(right, rightKeys, joinRequest.isNullSafe(), sparkSchema, joinPartitions);
        // only need to repartition the left side if this is the first join,
        // as intermediate joins will already be partitioned on the key
        if (joined == left) {
          joined = partitionOnKey(joined, leftKeys, joinRequest.isNullSafe(),
                                  leftSparkSchema, joinPartitions);
        }
      }
      joined = joined.join(right, joinOn, joinType);

      /*
           Consider stages A, B, C:

           A (id, email) = (2, charles@example.com)
           B (id, name) = (0, alice), (1, bob)
           C (id, age) = (0, 25)

           where A, B, C are joined on A.id = B.id = C.id, where B and C are required and A is optional.
           This RDDCollection is the data for stage A.

           this is implemented as a join of (A right outer join B on A.id = B.id) as TMP1
           followed by (TMP1 inner join C on TMP1.B.id = C.id) as OUT

           TMP1 looks like:
           TMP1 (A.id, A.name, B.id, B.email) = (null, null, 0, alice), (null, null, 1, bob)

           and the final output looks like:
           OUT (A.id, A.name, B.id, B.email, C.id, C.age) = (null, null, 0, alice, 0, 25)

           It's important to join on B.id = C.id and not on A.id = C.id, because joining on A.id = C.id will result
           in an empty output, as A.id is always null in the TMP1 dataset. In general, the principle is to join on the
           required fields and not on the optional fields when possible.
       */
      /*
           Additionally if none of the datasets are required until now, which means all of the joines will outer.
           In this case also we need to pass on the join columns as we need to compare using coalesce of all previous
           columns with the right dataset
       */
      if (toJoin.isRequired() || !seenRequired) {
        leftJoinColumns = rightJoinColumns;
      }
    }

    // select and alias fields in the expected order
    List<Column> outputColumns = new ArrayList<>(joinRequest.getFields().size());
    for (JoinField field : joinRequest.getFields()) {
      Column column = collections.get(field.getStageName()).col(field.getFieldName());
      if (field.getAlias() != null) {
        column = column.alias(field.getAlias());
      }
      outputColumns.add(column);
    }

    Seq<Column> outputColumnSeq = JavaConversions.asScalaBuffer(outputColumns).toSeq();
    joined = joined.select(outputColumnSeq);

    Schema outputSchema = joinRequest.getOutputSchema();
    JavaRDD<StructuredRecord> output = joined.javaRDD()
      .map(r -> DataFrames.fromRow(r, outputSchema))
      .map(new CountingFunction<>(stageName, sec.getMetrics(), Constants.Metrics.RECORDS_OUT,
                                  sec.getDataTracer(stageName)));
    return (SparkCollection<T>) wrap(output);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> join(JoinExpressionRequest joinRequest) {
    Function<StructuredRecord, StructuredRecord> recordsInCounter =
      new CountingFunction<>(joinRequest.getStageName(), sec.getMetrics(), Constants.Metrics.RECORDS_IN,
                             sec.getDataTracer(joinRequest.getStageName()));

    JoinCollection leftInfo = joinRequest.getLeft();
    StructType leftSchema = DataFrames.toDataType(leftInfo.getSchema());
    Dataset<Row> leftDF = toDataset(((JavaRDD<StructuredRecord>) rdd).map(recordsInCounter), leftSchema);

    JoinCollection rightInfo = joinRequest.getRight();
    SparkCollection<?> rightData = rightInfo.getData();
    StructType rightSchema = DataFrames.toDataType(rightInfo.getSchema());
    Dataset<Row> rightDF = toDataset(((JavaRDD<StructuredRecord>) rightData.getUnderlying()).map(recordsInCounter),
                                     rightSchema);

    // if this is not a broadcast join, Spark will reprocess each side multiple times, depending on the number
    // of partitions. If the left side has N partitions and the right side has M partitions,
    // the left side gets reprocessed M times and the right side gets reprocessed N times.
    // Cache the input to prevent confusing metrics and potential source re-reading.
    // this is only necessary for inner joins, since outer joins are automatically changed to
    // BroadcastNestedLoopJoins by Spark
    boolean isInner = joinRequest.getLeft().isRequired() && joinRequest.getRight().isRequired();
    boolean isBroadcast = joinRequest.getLeft().isBroadcast() || joinRequest.getRight().isBroadcast();
    if (isInner && !isBroadcast) {
      leftDF = leftDF.persist(StorageLevel.DISK_ONLY());
      rightDF = rightDF.persist(StorageLevel.DISK_ONLY());
    }

    // register using unique names to avoid collisions.
    String leftId = UUID.randomUUID().toString().replaceAll("-", "");
    String rightId = UUID.randomUUID().toString().replaceAll("-", "");
    leftDF.registerTempTable(leftId);
    rightDF.registerTempTable(rightId);

    /*
        Suppose the join was originally:

          select P.id as id, users.name as username
          from purchases as P join users
          on P.user_id = users.id or P.user_id = 0

        After registering purchases as uuid0 and users as uuid1,
        the query needs to be rewritten to replace the original names with the new generated ids,
        as the query needs to be:

          select P.id as id, uuid1.name as username
          from uuid0 as P join uuid1
          on P.user_id = uuid1.id or P.user_id = 0
     */
    String sql = getSQL(joinRequest.rename(leftId, rightId));
    LOG.debug("Executing join stage {} using SQL: \n{}", joinRequest.getStageName(), sql);
    Dataset<Row> joined = sqlContext.sql(sql);

    Schema outputSchema = joinRequest.getOutputSchema();
    JavaRDD<StructuredRecord> output = joined.javaRDD()
      .map(r -> DataFrames.fromRow(r, outputSchema))
      .map(new CountingFunction<>(joinRequest.getStageName(), sec.getMetrics(),
                                  Constants.Metrics.RECORDS_OUT,
                                  sec.getDataTracer(joinRequest.getStageName())));
    return (SparkCollection<T>) wrap(output);
  }

  /**
   * Helper method that adds a salt column to a dataframe for join distribution
   *
   * @param data               Dataframe add salt to
   * @param saltColumnName     Name to use for the new salt column
   * @param distributionFactor The desired salt size, values in the salt column will range [0,distributionFactor)
   * @return Dataframe with an additional salt column
   */
  private Dataset saltDataset(Dataset data, String saltColumnName, int distributionFactor) {
    Dataset saltedData = data.withColumn(saltColumnName, functions.rand().multiply(distributionFactor));
    saltedData = saltedData.withColumn(saltColumnName,
                                       functions.floor(saltedData.col(saltColumnName)).cast(DataTypes.IntegerType));
    return saltedData;
  }

  /**
   * Helper method that adds salt column to a dataframe and explodes the rows
   *
   * @param data               Dataframe to explode
   * @param saltColumnName     Name to use for the new salt column
   * @param distributionFactor The desired salt size, this will increase the number of rows by a factor of
   *                           distributionFactor
   * @return Dataframe with an additional salt column
   */
  private Dataset explodeDataset(Dataset data, String saltColumnName, int distributionFactor) {
    //Array of [0,distributionFactor) to be used in to prepare for the explode
    Integer[] numbers = IntStream.range(0, distributionFactor).boxed().toArray(Integer[]::new);

    // Add a column that uses the 'numbers' array as the value for every row
    Dataset explodedData = data.withColumn(saltColumnName,
                                           functions.array(
                                             Arrays.stream(numbers).map(functions::lit).toArray(Column[]::new)
                                           ));
    explodedData = explodedData.withColumn(saltColumnName, functions.explode(explodedData.col(saltColumnName)));
    return explodedData;
  }

  protected Dataset<Row> toDataset(JavaRDD<StructuredRecord> rdd, StructType sparkSchema) {
    JavaRDD<Row> rowRDD = rdd.map(record -> DataFrames.toRow(record, sparkSchema));
    return sqlContext.createDataFrame(rowRDD.rdd(), sparkSchema);
  }

  private Dataset<Row> partitionOnKey(Dataset<Row> df, List<String> key, boolean isNullSafe, StructType sparkSchema,
                                      int numPartitions) {
    List<Column> columns = getPartitionColumns(df, key, isNullSafe, sparkSchema);
    return df.repartition(numPartitions, JavaConversions.asScalaBuffer(columns).toSeq());
  }

  private List<Column> getPartitionColumns(Dataset<Row> df, List<String> key, boolean isNullSafe,
                                           StructType sparkSchema) {
    if (!isNullSafe) {
      return key.stream().map(df::col).collect(Collectors.toList());
    }

    // if a null safe join is happening, spark will partition on coalesce(col, [default val]),
    // where the default val is dependent on the column type and defined in
    // org.apache.spark.sql.catalyst.expressions.Literal
    return key.stream().map(keyCol -> {
      int fieldIndex = sparkSchema.fieldIndex(keyCol);
      DataType dataType = sparkSchema.fields()[fieldIndex].dataType();
      Column defaultCol = new Column(LiteralsBridge.defaultLiteral(dataType));
      return functions.coalesce(df.col(keyCol), defaultCol);
    }).collect(Collectors.toList());
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec,
                                                             @Nullable Integer partitions,
                                                             StageStatisticsCollector collector) {
    if (!useDatasetAggregation) {
      return super.reduceAggregate(stageSpec, partitions, collector);
    }
    return this.reduceDatasetAggregate(stageSpec, partitions, collector);
  }

  /**
   * helper function to provide a generified encoder for any serializable type
   */
  private <V> Encoder<V> objectEncoder() {
    return useKryoForDatasets ? KRYO_OBJECT_ENCODER : JAVA_OBJECT_ENCODER;
  }

  /**
   * helper function to provide a generified encoder for tuple of two serializable types
   */
  private <V1, V2> Encoder<Tuple2<V1, V2>> tupleEncoder() {
    return useKryoForDatasets ? KRYO_TUPLE_ENCODER : JAVA_TUPLE_ENCODER;
  }

  /**
   * Performs reduce aggregate using Dataset API. This allows SPARK to perform various optimizations that
   * are not available when working on the RDD level.
   */
  private <GROUP_KEY, AGG_VALUE> SparkCollection<RecordInfo<Object>> reduceDatasetAggregate(
    StageSpec stageSpec, @Nullable Integer partitions, StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    DatasetAggregationGetKeyFunction<GROUP_KEY, T, AGG_VALUE> groupByFunction = new DatasetAggregationGetKeyFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());
    DatasetAggregationReduceFunction<T, AGG_VALUE> reduceFunction = new DatasetAggregationReduceFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());
    DatasetAggregationFinalizeFunction<GROUP_KEY, T, AGG_VALUE, ?> postFunction =
      new DatasetAggregationFinalizeFunction<>(pluginFunctionContext, functionCacheFactory.newCache());
    MapFunction<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<T, AGG_VALUE>>, GROUP_KEY> keyFromTuple = Tuple2::_1;
    MapFunction<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<T, AGG_VALUE>>,
      DatasetAggregationAccumulator<T, AGG_VALUE>> valueFromTuple = Tuple2::_2;

    Dataset<T> dataset = sqlContext.createDataset(rdd.rdd(), objectEncoder());

    Dataset<RecordInfo<Object>> groupedDataset = dataset
      .flatMap(groupByFunction, tupleEncoder())
      .groupByKey(keyFromTuple, objectEncoder())
      .mapValues(valueFromTuple, objectEncoder())
      .reduceGroups(reduceFunction)
      .flatMap(postFunction, objectEncoder());

    if (!ignorePartitionsDuringDatasetAggregation && partitions != null) {
      groupedDataset = groupedDataset.coalesce(partitions);
    }

    return wrap(groupedDataset.toJavaRDD());
  }

  private void collectLeftJoinOnCols(List<List<Column>> listOfListOfColumns, int index, Column leftJoinOnCurrent) {
    if (listOfListOfColumns.size() <= index) {
      listOfListOfColumns.add(new ArrayList<Column>());
    }
    listOfListOfColumns.get(index).add(leftJoinOnCurrent);
  }

  private Column getLeftJoinOnCoalescedColumn(Column leftJoinOnCurrent, int index,
                                              List<List<Column>> listOfListOfColumns) {
    Column[] colArray = new Column[listOfListOfColumns.get(index).size()];
    Column coalesedCol = coalesce(listOfListOfColumns.get(index).toArray(colArray));
    return coalesedCol;
  }
}
