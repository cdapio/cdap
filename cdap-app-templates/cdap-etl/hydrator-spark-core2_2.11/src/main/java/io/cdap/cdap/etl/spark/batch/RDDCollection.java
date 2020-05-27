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
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.join.JoinCollection;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    Dataset<Row> left = toDataset((JavaRDD<StructuredRecord>) rdd, joinRequest.getLeftSchema());
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
        Join #3 is an inner join because even though it contains 2 optional datasets, because 'B' is still required.
     */
    boolean seenRequired = joinRequest.isLeftRequired();
    Dataset<Row> joined = left;
    for (JoinCollection toJoin : joinRequest.getToJoin()) {
      RDDCollection<StructuredRecord> data = (RDDCollection<StructuredRecord>) toJoin.getData();
      Dataset<Row> right = toDataset(data.rdd, toJoin.getSchema());
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
    JavaRDD<StructuredRecord> output = joined.javaRDD().map(r -> DataFrames.fromRow(r, outputSchema));
    return (SparkCollection<T>) wrap(output);
  }

  private Column eq(Column left, Column right, boolean isNullSafe) {
    if (isNullSafe) {
      return left.eqNullSafe(right);
    }
    return left.equalTo(right);
  }

  private Dataset<Row> toDataset(JavaRDD<StructuredRecord> rdd, Schema schema) {
    StructType sparkSchema = DataFrames.toDataType(schema);
    JavaRDD<Row> rowRDD = rdd.map(record -> DataFrames.toRow(record, sparkSchema));
    return sqlContext.createDataset(rowRDD.rdd(), RowEncoder.apply(sparkSchema));
  }
}
