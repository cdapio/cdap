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
import static org.junit.Assert.assertTrue;


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


  List<Map<String,Object>> selectEmail(Connection connection, String email) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.select("account").includeAll().where("email_id").equal(email).execute();
  }



  public boolean deleteOne(Connection connection, String email) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.delete("ACCOUNT").where("email_id").equal(email).execute();

  }

  public boolean deleteAll(Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.delete("ACCOUNT").noWhere().execute();

  }

  public boolean updateManyColumns (Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.update("account").set("company","Continuuity").set("email_id","sree@continuuity.com")
                .setLast("name","Sree").where("email_id").equal("sree@gmail.com").execute();
  }

  public boolean updateOne(Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.update("account").setLast("name","Sreevatsan Raman").where("name").equal("sree").execute();
  }

  @Test
  public void testInsertAndSelect() throws SQLException, ClassNotFoundException {

    //Startup HSQL instance
    TestHelper.startHsqlDB();
    Connection connection =DriverManager.getConnection("jdbc:hsqldb:mem:test;" +
                             "hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false", "sa", "");

    //Insert
    //PreparedStatment's execute returns false on inserts
    assertFalse(insertSingle(connection,"sree@gmail.com","sree"));

    //Select all
    assertEquals(1,selectAll(connection).size() );

    //Select none
    assertEquals(0, selectNone(connection).size());

    //Insert again
    assertFalse(insertSingle(connection,"simpson@homer.com","homer simpson"));

    //Select by name
    assertEquals(1,selectName(connection,"homer simpson").size());

    //Select all count 2
    assertEquals(2, selectAll(connection).size());

    //Delete one
    assertFalse(deleteOne(connection,"simpson@homer.com"));

    //Verify the data is deleted
    assertEquals(1, selectAll(connection).size());


    //Update an entry
    assertFalse(updateOne(connection));

    //Verify Updates
    List<Map<String,Object>> data  = selectEmail(connection,"sree@gmail.com");
    for  (Map<String,Object> d : data){
      assertEquals("Sreevatsan Raman",d.get("NAME"));
    }

    //Update multiple Entries
    assertFalse(updateManyColumns(connection));

    //Verify result of updating multiple columns
     data  = selectEmail(connection,"sree@continuuity.com");
    for  (Map<String,Object> d : data){
      assertEquals("Sree",d.get("NAME"));
      assertEquals("Continuuity",d.get("COMPANY"));
    }

    //Delete all
    assertFalse(deleteAll(connection));

    //Verify if delete all worked
    assertEquals(0,selectAll(connection).size());

    //Stop HSQL instance
    TestHelper.stopHsqlDB();

  }

}
