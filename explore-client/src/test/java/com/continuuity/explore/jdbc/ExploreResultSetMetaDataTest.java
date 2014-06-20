package com.continuuity.explore.jdbc;

import com.continuuity.explore.service.ColumnDesc;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;

/**
 *
 */
public class ExploreResultSetMetaDataTest {

  @Test
  public void metaDataTest() throws Exception {
    // TODO test all the types?
    ExploreResultSetMetaData metaData = new ExploreResultSetMetaData(Lists.newArrayList(
        new ColumnDesc("foo", "STRING", 1, ""),
        new ColumnDesc("bar", "int", 2, "")
    ));

    // getColumnCount
    Assert.assertEquals(2, metaData.getColumnCount());

    // getColumnTypeName
    Assert.assertEquals("string", metaData.getColumnTypeName(1));
    Assert.assertEquals("int", metaData.getColumnTypeName(2));

    // getColumnType
    Assert.assertEquals(Types.VARCHAR, metaData.getColumnType(1));
    Assert.assertEquals(Types.INTEGER, metaData.getColumnType(2));

    // getColumnClassName
    Assert.assertEquals(String.class.getName(), metaData.getColumnClassName(1));
    Assert.assertEquals(Integer.class.getName(), metaData.getColumnClassName(2));

    // getColumnName, getColumnLabel
    Assert.assertEquals("foo", metaData.getColumnName(1));
    Assert.assertEquals("foo", metaData.getColumnLabel(1));
    Assert.assertEquals("bar", metaData.getColumnName(2));
    Assert.assertEquals("bar", metaData.getColumnLabel(2));

  }
}
