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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.request.SQLRelationDefinition;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.spark.batch.relation.SparkSQLRelation;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SparkSQLEngineTest {

  @Test
  public void testGetRelation() throws IOException {
    //Prepare Schema
    String schemaStr = "{\"type\":\"record\",\"name\":\"text\"," +
      "\"fields\":" +
      "[{\"name\":\"name\",\"type\":\"string\"}," +
      "{\"name\":\"age\",\"type\":\"int\"}]}";

    SQLRelationDefinition relationDefinition =
      new SQLRelationDefinition("testDataset", Schema.parseJson(schemaStr));

    Relation relation = new SparkSQLEngine().getRelation(relationDefinition);

    Assert.assertTrue(relation instanceof SparkSQLRelation);

    List<String> actualCols = ((SparkSQLRelation) relation).getColumns();
    List<String> expectedCols = Arrays.asList("name", "age");

    //Check the columns according the above defined Schema
    Assert.assertEquals(expectedCols, actualCols);
  }
}
