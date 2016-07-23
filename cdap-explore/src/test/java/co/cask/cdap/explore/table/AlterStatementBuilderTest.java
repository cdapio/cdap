/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.common.utils.ProjectInfo;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AlterStatementBuilderTest {

  LocationFactory locationFactory = new LocalLocationFactory();

  @Test
  public void testWithLocation() throws Exception {
    Location location = locationFactory.create("/some/path");
    String expected = "ALTER TABLE dataset_xyz " +
      "SET LOCATION '" + location.toURI().toString() + "'";

    String actual = new AlterStatementBuilder("xyz", "dataset_xyz", true)
      .buildWithLocation(location);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWithTableProperties() throws Exception {
    String expected = "ALTER TABLE dataset_xyz " +
      "SET TBLPROPERTIES ('somekey'='someval', 'cdap.name'='xyz', " +
      "'cdap.version'='" + ProjectInfo.getVersion().toString() + "')";

    String actual = new AlterStatementBuilder("xyz", "dataset_xyz", true)
      .buildWithTableProperties(ImmutableMap.of("somekey", "someval"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWithSchema() throws Exception {
    Schema schema = Schema.recordOf(
      "stuff",
      Schema.Field.of("f1", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("f2", Schema.of(Schema.Type.INT)),
      Schema.Field.of("f3", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("f4", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("f5", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("f6", Schema.of(Schema.Type.BYTES)));

    // escape = false
    String expected = "ALTER TABLE dataset_xyz " +
      "REPLACE COLUMNS (f1 string, f2 int, f3 double, f4 boolean, f5 float, f6 binary)";
    String actual = new AlterStatementBuilder("xyz", "dataset_xyz", false)
      .buildWithSchema(schema);
    Assert.assertEquals(expected, actual);

    // escape true
    expected = "ALTER TABLE dataset_xyz " +
      "REPLACE COLUMNS (`f1` string, `f2` int, `f3` double, `f4` boolean, `f5` float, `f6` binary)";
    actual = new AlterStatementBuilder("xyz", "dataset_xyz", true)
      .buildWithSchema(schema);
    Assert.assertEquals(expected, actual);

    // escape true, schema given as string
    expected = "ALTER TABLE dataset_xyz " +
      "REPLACE COLUMNS (f1 string, f2 int, f3 double)";
    actual = new AlterStatementBuilder("xyz", "dataset_xyz", true)
      .buildWithSchema("f1 string, f2 int, f3 double");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWithFileFormat() throws Exception {
    String expected = "ALTER TABLE dataset_xyz SET FILEFORMAT TEXTFILE";
    String actual = new AlterStatementBuilder("xyz", "dataset_xyz", true)
      .buildWithFileFormat("TEXTFILE");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWithDelimiter() throws Exception {
    String expected = "ALTER TABLE dataset_xyz SET SERDEPROPERTIES ('field.delim'='|')";
    String actual = new AlterStatementBuilder("xyz", "dataset_xyz", true)
      .buildWithDelimiter("|");
    Assert.assertEquals(expected, actual);

    expected = "ALTER TABLE dataset_xyz SET SERDEPROPERTIES ('field.delim'='\\001')";
    actual = new AlterStatementBuilder("xyz", "dataset_xyz", true)
      .buildWithDelimiter(null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWithFormats() throws Exception {
    String expected = "ALTER TABLE dataset_xyz SET FILEFORMAT " +
      "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
      "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
      "SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'";
    String actual = new AlterStatementBuilder("xyz", "dataset_xyz", true)
      .buildWithFormats("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
                        "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
                        "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
    Assert.assertEquals(expected, actual);
  }



}
