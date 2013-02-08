package com.continuuity.passport.dal;


import com.continuuity.passport.common.sql.SQLChainImpl;
import com.continuuity.passport.common.sql.clause.SQLChain;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 *  Various test cases for SQL functions
 */
public class TestSQLInsert {


  public boolean insertSingle(Connection connection,String email, String name) throws SQLException {

    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.insert("account").columns("name,email_id").
      values(name, email).execute();

  }

  List<Map<String,Object>> selectAll(Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.select("account").includeAll().noWhere().execute();
  }

  List<Map<String,Object>> selectNone(Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.select("account").includeAll().where("email_id").equal("foo@bar.com").execute();
  }

  List<Map<String,Object>> selectName(Connection connection, String name) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.select("account").includeAll().where("name").equal(name).execute();
  }




  @Test
  public void testInsertAndSelect() throws SQLException, ClassNotFoundException {

    //Startup HSQL instance
    TestHelper.startHsqlDB();
    Connection connection =DriverManager.getConnection("jdbc:hsqldb:mem:test;" +
                             "hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false", "sa", "");

    //Insert
    //PreparedStatment's execute returns false on inserts
    assertFalse(insertSingle(connection,"sree@continuuity.com","sree"));


    //Select all
    assertEquals(1,selectAll(connection).size() );


    //Select none
    assertEquals(0, selectNone(connection).size());

    //Insert again
    assertFalse(insertSingle(connection,"simpson@homer.com","homer simpson"));

    //Select by name
//    assertEquals(1,selectName(connection,"homer simpson").size());

    //Select all count 2
    assertEquals(2, selectAll(connection).size());


     //Stop HSQL instance
    TestHelper.stopHsqlDB();

  }

}
