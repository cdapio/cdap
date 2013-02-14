package com.continuuity.passport.dal;


import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 *  Various test cases for SQL functions
 */
public class TestSQLInsert {


  @Test
  public void testInsertAndSelect() throws SQLException, ClassNotFoundException {


    //Startup HSQL instance
    TestHelper.startHsqlDB();
    Connection connection =DriverManager.getConnection("jdbc:hsqldb:mem:test;" +
                             "hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false", "sa", "");


    //TODO: Add test cases
    //Note: Removed SQLCHain related test cases should add in DAL specific test cases.

    //Stop HSQL instance
    TestHelper.stopHsqlDB();

  }

}
