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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
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
 * Spark1 RDD collection.
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
    Map<String, DataFrame> collections = new HashMap<>();
    DataFrame left = toDataFrame((JavaRDD<StructuredRecord>) rdd, joinRequest.getLeftSchema());
    collections.put(joinRequest.getLeftStage(), left);

    List<Column> leftJoinColumns = joinRequest.getLeftKey().stream()
      .map(left::col)
      .collect(Collectors.toList());

    DataFrame joined = left;
    for (JoinCollection toJoin : joinRequest.getToJoin()) {
      RDDCollection<StructuredRecord> data = (RDDCollection<StructuredRecord>) toJoin.getData();
      DataFrame right = toDataFrame(data.rdd, toJoin.getSchema());
      collections.put(toJoin.getStage(), right);

      List<Column> rightJoinColumns = toJoin.getKey().stream()
        .map(right::col)
        .collect(Collectors.toList());

      Iterator<Column> leftIter = leftJoinColumns.iterator();
      Iterator<Column> rightIter = rightJoinColumns.iterator();
      Column joinOn = leftIter.next().equalTo(rightIter.next());
      while (leftIter.hasNext()) {
        joinOn = joinOn.and(leftIter.next().equalTo(rightIter.next()));
      }

      String joinType = toJoin.getType();
      joined = joined.join(right, joinOn, joinType);
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

  private DataFrame toDataFrame(JavaRDD<StructuredRecord> rdd, Schema schema) {
    StructType sparkSchema = DataFrames.toDataType(schema);
    JavaRDD<Row> rowRDD = rdd.map(record -> DataFrames.toRow(record, sparkSchema));
    return sqlContext.createDataFrame(rowRDD.rdd(), sparkSchema);
  }
}
