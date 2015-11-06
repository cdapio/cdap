/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkSqlApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSqlApp.class);
  public static final String TRAIN_HISTORY_DS = "TrainHistory";
  public static final String OFFER_DS = "Offer";
  public static final String DATASET_SCHEMA_ARG = "datasets.schema";
  public static final String SQL_ARG = "sql";

  @Override
  public void configure() {
    addSpark(new SparkSqlProgram());
    createDataset(TRAIN_HISTORY_DS, Table.class);
    createDataset(OFFER_DS, Table.class);
  }

  public static class SparkSqlProgram extends AbstractSpark {
    @Override
    protected void configure() {
      setMainClass(JavaSparkSql.class);
    }
  }

  public static class JavaSparkSql implements JavaSparkProgram {

    private static final long serialVersionUID = 6877153909627453488L;

    @Override
    public void run(SparkContext context) throws Exception {
      Map<String, Map<String, String>> datasets = new Gson()
        .fromJson(context.getRuntimeArguments().get(DATASET_SCHEMA_ARG),
                  new TypeToken<Map<String, Map<String, String>>>() { }.getType());

      SQLContext sqlContext = new SQLContext((JavaSparkContext) context.getOriginalSparkContext());

      for (Map.Entry<String, Map<String, String>> entry : datasets.entrySet()) {
        String dataset = entry.getKey();

        // Generate schema
        List<StructField> fields = new ArrayList<>();
        final Map<String, String> dataTypes = entry.getValue();
        for (Map.Entry<String, String> field : dataTypes.entrySet()) {
          fields.add(DataTypes.createStructField(field.getKey(), createDataType(field.getValue()), true));
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert to Row
        JavaPairRDD<byte[], co.cask.cdap.api.dataset.table.Row> rdd =
          context.readFromDataset(dataset, byte[].class, co.cask.cdap.api.dataset.table.Row.class);
        JavaRDD<Row> table = rdd.values().map(new Function<co.cask.cdap.api.dataset.table.Row, Row>() {
          @Override
          public Row call(co.cask.cdap.api.dataset.table.Row row) throws Exception {
            Map<byte[], byte[]> columns = row.getColumns();
            Object[] colValues = new Object[columns.size()];
            int i = 0;
            for (Map.Entry<String, String> dataTypeEntry : dataTypes.entrySet()) {
              String name = dataTypeEntry.getKey();
              String dataType = dataTypeEntry.getValue();
              byte[] value = columns.get(Bytes.toBytes(name));
              colValues[i++] = convert(name, value, dataType);
            }
            return RowFactory.create(colValues);
          }
        });

        DataFrame dataFrame = sqlContext.createDataFrame(table, schema);
        dataFrame.registerTempTable(dataset);
      }

      DataFrame result = sqlContext.sql(context.getRuntimeArguments().get(SQL_ARG));
      int count = 0;
      for (Row row : result.collectAsList()) {
        System.out.println(row.toString());
        count++;
      }
      System.out.println("Result count: " + count);

      System.out.println("++++++++++++++++++++++++++ TrainHistory ++++++++++++++++++++++++++++++++++++++++");
      DataFrame trainHistoryDf = sqlContext.table(TRAIN_HISTORY_DS);
      trainHistoryDf.describe();
      System.out.println("Showing trainhistory");
      trainHistoryDf.show();
      System.out.println("cols = " + Joiner.on(",").join(trainHistoryDf.columns()));
      System.out.println("++++++++++++++++++++++++++ Offer +++++++++++++++++++++++++++++++++++++++++++++++");
      DataFrame offerDf = sqlContext.table(OFFER_DS);
      offerDf.describe();
      System.out.println("Showing offer");
      offerDf.show();
      System.out.println("cols = " + Joiner.on(",").join(offerDf.columns()));
      System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
      Column idCol = trainHistoryDf.col("id");
      Column offerIdCol = offerDf.col("id");
      DataFrame join = trainHistoryDf.join(offerDf, idCol.equalTo(offerIdCol));
      join.explain(true);
      System.out.println("Showing join");
      join.show();
      join.describe();
      System.out.println("Select and show join");
      join.select().show();
    }

    private Object convert(String colName, byte[] value, String dataType) {
      Schema.Type type = Schema.Type.valueOf(dataType.toUpperCase());
      switch (type) {
        case BOOLEAN:
          return Bytes.toBoolean(value);
        case INT:
          return Bytes.toInt(value);
        case LONG:
          return Bytes.toLong(value);
        case FLOAT:
          return Bytes.toFloat(value);
        case DOUBLE:
          return Bytes.toDouble(value);
        case BYTES:
          return value;
        case STRING:
          return Bytes.toString(value);
      }
      throw new IllegalArgumentException("Only Boolean, int, long, float, double, bytes and string are supported. " +
                                           "Got: " + dataType);
    }

    private DataType createDataType(String schemaStr) {
      try {
        Schema schema = Schema.of(Schema.Type.valueOf(schemaStr.toUpperCase()));
        switch (schema.getType()) {
          case NULL:
            return DataTypes.NullType;
          case BOOLEAN:
            return DataTypes.BooleanType;
          case INT:
            return DataTypes.IntegerType;
          case LONG:
            return DataTypes.LongType;
          case FLOAT:
            return DataTypes.FloatType;
          case DOUBLE:
            return DataTypes.DoubleType;
          case BYTES:
            return DataTypes.BinaryType;
          case STRING:
            return DataTypes.StringType;
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      throw new IllegalArgumentException("Unsupported schema: " + schemaStr);
    }
  }
}
