/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.explore.table;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.common.utils.ProjectInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CreateStatementBuilderTest {

  @Test
  public void testRowDelimitedCreate() throws Exception {
    String expected = "CREATE EXTERNAL TABLE IF NOT EXISTS abc.dataset_myfiles " +
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

    String actual = new CreateStatementBuilder("myfiles", "abc", "dataset_myfiles", false)
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

    String actual = new CreateStatementBuilder("myfiles", null, "dataset_myfiles", false)
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
      Schema.Field.of("f3", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
      Schema.Field.of("ts1", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
      Schema.Field.of("ts2", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)))
    );
    String expected = "CREATE EXTERNAL TABLE IF NOT EXISTS dataset_myfiles (f1 string, f2 int, f3 double, " +
      "dt date, ts1 timestamp, ts2 timestamp) " +
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

    String actual = new CreateStatementBuilder("myfiles", null, "dataset_myfiles", false)
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
