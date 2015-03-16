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

package co.cask.cdap.explore.table;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class CreateStatementBuilderTest {

  @Test
  public void testStorageHandlerCreate() throws Exception {
    String expected = "CREATE EXTERNAL TABLE IF NOT EXISTS stream_purchases " +
      "(f1 string, f2 int, f3 double, f4 boolean, f5 float, f6 binary) COMMENT 'CDAP Stream' " +
      "STORED BY 'co.cask.cdap.hive.stream.StreamStorageHandler' " +
      "WITH SERDEPROPERTIES ('explore.stream.name'='purchases', 'explore.stream.namespace'='default') " +
      "LOCATION 'hdfs://namenode/my/path' " +
      "TBLPROPERTIES ('somekey'='someval', 'cdap.name'='purchases', " +
                      "'cdap.version'='" + ProjectInfo.getVersion().toString() + "')";
    Schema schema = Schema.recordOf(
      "stuff",
      Schema.Field.of("f1", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("f2", Schema.of(Schema.Type.INT)),
      Schema.Field.of("f3", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("f4", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("f5", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("f6", Schema.of(Schema.Type.BYTES)));
    Map<String, String> serdeProperties = ImmutableMap.of(
      Constants.Explore.STREAM_NAME, "purchases",
      Constants.Explore.STREAM_NAMESPACE, "default");

    String actual = new CreateStatementBuilder("purchases", "stream_purchases")
      .setSchema(schema)
      .setLocation("hdfs://namenode/my/path")
      .setTableProperties(ImmutableMap.of("somekey", "someval"))
      .setTableComment("CDAP Stream")
      .buildWithStorageHandler(Constants.Explore.STREAM_STORAGE_HANDLER_CLASS, serdeProperties);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRowDelimitedCreate() throws Exception {
    String expected = "CREATE EXTERNAL TABLE IF NOT EXISTS dataset_myfiles " +
      "(f1 string, f2 int, f3 double, f4 binary, f5 array<int>) COMMENT 'CDAP Dataset' " +
      "PARTITIONED BY (f1 STRING, f2 INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "STORED AS TEXTFILE LOCATION 'hdfs://namenode/my/path' " +
      "TBLPROPERTIES ('cdap.name'='myfiles', 'cdap.version'='" + ProjectInfo.getVersion().toString() + "')";
    String hiveSchema = "f1 string, f2 int, f3 double, f4 binary, f5 array<int>";
    Partitioning partitioning = Partitioning.builder()
      .addStringField("f1")
      .addIntField("f2")
      .build();

    String actual = new CreateStatementBuilder("myfiles", "dataset_myfiles")
      .setSchema(hiveSchema)
      .setLocation("hdfs://namenode/my/path")
      .setTableComment("CDAP Dataset")
      .setPartitioning(partitioning)
      .setRowFormatDelimited(",", null)
      .buildWithFileFormat("TEXTFILE");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRowSerdeCreate() throws Exception {
    String expected = "CREATE EXTERNAL TABLE IF NOT EXISTS dataset_myfiles " +
      "(f1 string, f2 int, f3 double, f4 binary, f5 array<int>) COMMENT 'CDAP Dataset' " +
      "PARTITIONED BY (f1 STRING, f2 INT) " +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' " +
      "WITH SERDEPROPERTIES ('input.regex'='escapeme!\\'') " +
      "STORED AS TEXTFILE LOCATION 'hdfs://namenode/my/path' " +
      "TBLPROPERTIES ('cdap.name'='myfiles', 'cdap.version'='" + ProjectInfo.getVersion().toString() + "')";
    String hiveSchema = "f1 string, f2 int, f3 double, f4 binary, f5 array<int>";
    Partitioning partitioning = Partitioning.builder()
      .addStringField("f1")
      .addIntField("f2")
      .build();

    String actual = new CreateStatementBuilder("myfiles", "dataset_myfiles")
      .setSchema(hiveSchema)
      .setLocation("hdfs://namenode/my/path")
      .setTableComment("CDAP Dataset")
      .setPartitioning(partitioning)
      .setRowFormatSerde("org.apache.hadoop.hive.serde2.RegexSerDe", ImmutableMap.of("input.regex", "escapeme!'"))
      .buildWithFileFormat("TEXTFILE");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRowSerdeFormatsCreate() throws Exception {
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("f1", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("f2", Schema.of(Schema.Type.INT)),
      Schema.Field.of("f3", Schema.of(Schema.Type.DOUBLE))
    );
    String expected = "CREATE EXTERNAL TABLE IF NOT EXISTS dataset_myfiles (f1 string, f2 int, f3 double) " +
      "COMMENT 'CDAP Dataset' " +
      "PARTITIONED BY (f1 STRING, f2 INT) " +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
      "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
      "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
      "LOCATION 'hdfs://namenode/my/path' " +
      "TBLPROPERTIES ('avro.schema.literal'='" + schema.toString() + "', " +
      "'cdap.name'='myfiles', 'cdap.version'='" + ProjectInfo.getVersion().toString() + "')";
    Partitioning partitioning = Partitioning.builder()
      .addStringField("f1")
      .addIntField("f2")
      .build();

    String actual = new CreateStatementBuilder("myfiles", "dataset_myfiles")
      .setSchema(schema)
      .setTableProperties(ImmutableMap.of("avro.schema.literal", schema.toString()))
      .setLocation("hdfs://namenode/my/path")
      .setTableComment("CDAP Dataset")
      .setPartitioning(partitioning)
      .setRowFormatSerde("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .buildWithFormats("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
                        "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
    Assert.assertEquals(expected, actual);
  }
}
