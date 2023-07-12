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
                                                     "SELECT a AS a , b AS b , a+b AS c FROM testDataset " +
                                                     "AS testDataset",
                                                     null, baseSQLRelation);

    Assert.assertEquals(expectedRelation.getSqlStatement(), actualRelation.getSqlStatement());
  }

  @Test
  public void testDropColumn() {
    SparkSQLRelation actualRelation = (SparkSQLRelation) baseSQLRelation.dropColumn("b");
    SparkSQLRelation expectedRelation = new SparkSQLRelation("testDataset",
                                                             Arrays.asList("a"),
                                                             "SELECT a AS a FROM testDataset AS testDataset",
                                                             null, baseSQLRelation);
    Assert.assertEquals(expectedRelation.getSqlStatement(), actualRelation.getSqlStatement());
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
                                                               "testDataset AS testDataset",
                                                             null, baseSQLRelation);
    Assert.assertEquals(expectedRelation.getSqlStatement(), actualRelation.getSqlStatement());
  }

  @Test
  public void testFilter() {
    SparkSQLRelation actualRelation = (SparkSQLRelation) baseSQLRelation.filter(factory.compile("a > 2"));
    SparkSQLRelation expectedRelation = new SparkSQLRelation("testDataset",
                                                             Arrays.asList("a", "b"),
                                                             "SELECT a AS a , b AS b FROM testDataset AS testDataset " +
                                                               "WHERE a > 2",
                                                             null, baseSQLRelation);
    Assert.assertEquals(expectedRelation.getSqlStatement(), actualRelation.getSqlStatement());
  }

  @Test
  public void testNestedQuery() {
    SparkSQLRelation nestedRelation = (SparkSQLRelation) baseSQLRelation.filter(factory.compile("true"));
    SparkSQLRelation actualRelation = (SparkSQLRelation) nestedRelation.setColumn("c",factory.compile("a+b"));
    SparkSQLRelation expectedRelation = new SparkSQLRelation("testDataset",
                                                             Arrays.asList("a","b", "c"),
                                                             "SELECT a AS a , b AS b , a+b AS c FROM (SELECT a AS a " +
                                                             ", b AS b FROM testDataset AS testDataset WHERE true) " +
                                                             "AS testDataset",
                                                             null, nestedRelation);
    Assert.assertEquals(expectedRelation.getSqlStatement(), actualRelation.getSqlStatement());
  }

  @Test
  public void testNestedSelect() {
    Map<String, Expression> selectColumns = new LinkedHashMap<>();
    selectColumns.put("new_a", factory.compile("a"));
    SparkSQLRelation nestedRelation = (SparkSQLRelation) baseSQLRelation.select(selectColumns);
    selectColumns.put("new_b", factory.compile("b"));
    SparkSQLRelation actualRelation = (SparkSQLRelation) nestedRelation.select(selectColumns);

    SparkSQLRelation expectedRelation = new SparkSQLRelation("testDataset", Arrays.asList("new_a", "new_b"),
                                                             "SELECT a AS new_a , b AS new_b FROM (SELECT a AS new_a " +
                                                             "FROM testDataset AS testDataset) AS testDataset",
                                                             null, baseSQLRelation);
    Assert.assertEquals(expectedRelation.getSqlStatement(), actualRelation.getSqlStatement());
  }
}
