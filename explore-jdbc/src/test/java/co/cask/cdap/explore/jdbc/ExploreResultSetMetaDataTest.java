/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.explore.jdbc;

import co.cask.cdap.proto.ColumnDesc;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

/**
 *
 */
public class ExploreResultSetMetaDataTest {

  private void assertMetaData(List<ColumnDesc> columnDescs, List<String> expectedTypeNames, List<Integer> expectedTypes,
                              List<String> expectedClassNames, List<String> expectedNames)
      throws Exception {
    ResultSetMetaData metaData = new ExploreResultSetMetaData(columnDescs);
    Assert.assertEquals(columnDescs.size(), metaData.getColumnCount());
    for (int i = 0; i < expectedTypeNames.size(); i++) {
      Assert.assertEquals(expectedTypeNames.get(i), metaData.getColumnTypeName(i + 1));
    }
    for (int i = 0; i < expectedTypes.size(); i++) {
      Assert.assertEquals((long) expectedTypes.get(i), (long) metaData.getColumnType(i + 1));
    }
    for (int i = 0; i < expectedClassNames.size(); i++) {
      Assert.assertEquals(expectedClassNames.get(i), metaData.getColumnClassName(i + 1));
    }
    for (int i = 0; i < expectedNames.size(); i++) {
      Assert.assertEquals(expectedNames.get(i), metaData.getColumnName(i + 1));
      Assert.assertEquals(expectedNames.get(i), metaData.getColumnLabel(i + 1));
    }
  }

  @Test
  public void metaDataTest() throws Exception {
    assertMetaData(Lists.newArrayList(
        new ColumnDesc("foobar1", "STRING", 1, ""),
        new ColumnDesc("foobar2", "int", 2, ""),
        new ColumnDesc("foobar3", "char", 3, ""),
        new ColumnDesc("foobar4", "float", 4, ""),
        new ColumnDesc("foobar5", "double", 5, ""),
        new ColumnDesc("foobar6", "boolean", 6, ""),
        new ColumnDesc("foobar7", "tinyint", 7, ""),
        new ColumnDesc("foobar8", "smallint", 8, ""),
        new ColumnDesc("foobar9", "bigint", 9, ""),
        new ColumnDesc("foobar10", "date", 10, ""),
        new ColumnDesc("foobar11", "timestamp", 11, ""),
        new ColumnDesc("foobar12", "decimal", 12, ""),
        new ColumnDesc("foobar13", "binary", 13, ""),
        new ColumnDesc("foobar14", "map", 14, ""),
        new ColumnDesc("foobar15", "array", 15, ""),
        new ColumnDesc("foobar16", "struct", 16, "")
      ),
      Lists.newArrayList(
          "string",
          "int",
          "char",
          "float",
          "double",
          "boolean",
          "tinyint",
          "smallint",
          "bigint",
          "date",
          "timestamp",
          "decimal",
          "binary",
          "map",
          "array",
          "struct"
      ),
      Lists.newArrayList(
          Types.VARCHAR,
          Types.INTEGER,
          Types.CHAR,
          Types.FLOAT,
          Types.DOUBLE,
          Types.BOOLEAN,
          Types.TINYINT,
          Types.SMALLINT,
          Types.BIGINT,
          Types.DATE,
          Types.TIMESTAMP,
          Types.DECIMAL,
          Types.BINARY,
          Types.JAVA_OBJECT,
          Types.ARRAY,
          Types.STRUCT
      ),
      Lists.newArrayList(
          String.class.getName(),
          Integer.class.getName(),
          String.class.getName(),
          Float.class.getName(),
          Double.class.getName(),
          Boolean.class.getName(),
          Byte.class.getName(),
          Short.class.getName(),
          Long.class.getName(),
          Date.class.getName(),
          Timestamp.class.getName(),
          BigInteger.class.getName(),
          byte[].class.getName(),
          String.class.getName(),
          String.class.getName(),
          String.class.getName()
      ),
      Lists.newArrayList(
          "foobar1",
          "foobar2",
          "foobar3",
          "foobar4",
          "foobar5",
          "foobar6",
          "foobar7",
          "foobar8",
          "foobar9",
          "foobar10",
          "foobar11",
          "foobar12",
          "foobar13",
          "foobar14",
          "foobar15",
          "foobar16"
      )
    );

  }
}
