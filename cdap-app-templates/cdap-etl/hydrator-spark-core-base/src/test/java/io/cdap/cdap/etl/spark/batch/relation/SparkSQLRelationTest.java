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

package io.cdap.cdap.etl.spark.batch.relation;

import io.cdap.cdap.etl.api.relational.Expression;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class SparkSQLRelationTest {
  private SparkSQLExpressionFactory factory;
  private List<String> columns;
  private SparkSQLRelation baseSQLRelation;

  @Before
  public void setUp() {
    factory = new SparkSQLExpressionFactory();
    columns = new ArrayList<>(Arrays.asList("a", "b"));
    baseSQLRelation = new SparkSQLRelation("testDataset", columns);
  }

  @Test
  public void testSetColumn() {
    SparkSQLRelation actualRelation =
      (SparkSQLRelation) baseSQLRelation.setColumn("c", factory.compile("a+b"));

    SparkSQLRelation expectedRelation = new SparkSQLRelation("testDataset",
                                                     Arrays.asList("a", "b", "c"),
                                                     "SELECT a AS a , b AS b , a+b AS c FROM testDataset",
                                                     null, null);

    Assert.assertTrue(expectedRelation.equals(actualRelation));
  }

  @Test
  public void testDropColumn() {
    SparkSQLRelation actualRelation = (SparkSQLRelation) baseSQLRelation.dropColumn("b");
    SparkSQLRelation expectedRelation = new SparkSQLRelation("testDataset",
                                                             Arrays.asList("a"),
                                                             "SELECT a AS a FROM testDataset",
                                                             null,null);
    Assert.assertTrue(expectedRelation.equals(actualRelation));
  }

  @Test
  public void testSelect() {
    Map<String, Expression> selectColumns = new LinkedHashMap<>();
    selectColumns.put("new_a", factory.compile("a"));
    selectColumns.put("new_b", factory.compile("b"));

    SparkSQLRelation actualRelation = (SparkSQLRelation) baseSQLRelation.select(selectColumns);
    SparkSQLRelation expectedRelation = new SparkSQLRelation("testDataset",
                                                             Arrays.asList("new_a", "new_b"),
                                                             "SELECT a AS new_a , b AS new_b FROM " +
                                                               "testDataset",
                                                             null,null);
    Assert.assertTrue(expectedRelation.equals(actualRelation));
  }

  @Test
  public void testFilter() {
    SparkSQLRelation actualRelation = (SparkSQLRelation) baseSQLRelation.filter(factory.compile("a > 2"));
    SparkSQLRelation expectedRelation = new SparkSQLRelation("testDataset",
                                                             Arrays.asList("a", "b"),
                                                             "SELECT a AS a , b AS b FROM testDataset " +
                                                               "WHERE a > 2",
                                                             null, null);
    Assert.assertEquals(expectedRelation.getSqlStatement(), actualRelation.getSqlStatement());
  }
}
