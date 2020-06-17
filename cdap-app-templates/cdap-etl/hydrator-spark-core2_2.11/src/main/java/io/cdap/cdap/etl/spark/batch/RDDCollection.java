/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.function.CountingFunction;
import io.cdap.cdap.etl.spark.join.JoinCollection;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import io.cdap.cdap.etl.spark.plugin.LiteralsBridge;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Spark2 RDD collection.
 *
 * @param <T> type of object in the collection
 */
public class RDDCollection<T> extends BaseRDDCollection<T> {

  public RDDCollection(JavaSparkExecutionContext sec, JavaSparkContext jsc, SQLContext sqlContext,
                       DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory, JavaRDD<T> rdd) {
    super(sec, jsc, sqlContext, datasetContext, sinkFactory, rdd);
  }


  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> join(JoinRequest joinRequest) {
    Map<String, Dataset> collections = new HashMap<>();
    String stageName = joinRequest.getStageName();
    Function<StructuredRecord, StructuredRecord> recordsInCounter =
      new CountingFunction<>(stageName, sec.getMetrics(), Constants.Metrics.RECORDS_IN, sec.getDataTracer(stageName));
    StructType leftSparkSchema = DataFrames.toDataType(joinRequest.getLeftSchema());
    Dataset<Row> left = toDataset(stageName, ((JavaRDD<StructuredRecord>) rdd).map(recordsInCounter), leftSparkSchema);
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
    for (JoinCollection toJoin : joinRequest.getToJoin()) {
      RDDCollection<StructuredRecord> data = (RDDCollection<StructuredRecord>) toJoin.getData();
      StructType sparkSchema = DataFrames.toDataType(toJoin.getSchema());
      Dataset<Row> right = toDataset(stageName, data.rdd.map(recordsInCounter), sparkSchema);
      collections.put(toJoin.getStage(), right);

      List<Column> rightJoinColumns = toJoin.getKey().stream()
        .map(right::col)
        .collect(Collectors.toList());

      Iterator<Column> leftIter = leftJoinColumns.iterator();
      Iterator<Column> rightIter = rightJoinColumns.iterator();
      Column joinOn = eq(leftIter.next(), rightIter.next(), joinRequest.isNullSafe());
      while (leftIter.hasNext()) {
        joinOn = joinOn.and(eq(leftIter.next(), rightIter.next(), joinRequest.isNullSafe()));
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
      // spark.sql.shuffle.partitions setting in the spark conf
      if (joinPartitions != null && !toJoin.isBroadcast()) {
        right = partitionOnKey(right, toJoin.getKey(), joinRequest.isNullSafe(), sparkSchema, joinPartitions);
        // only need to repartition the left side if this is the first join,
        // as intermediate joins will already be partitioned on the key
        if (joined == left) {
          joined = partitionOnKey(joined, joinRequest.getLeftKey(), joinRequest.isNullSafe(),
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
      if (toJoin.isRequired()) {
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


  protected Dataset<Row> toDataset(String stageName, JavaRDD<StructuredRecord> rdd, StructType sparkSchema) {
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
}
