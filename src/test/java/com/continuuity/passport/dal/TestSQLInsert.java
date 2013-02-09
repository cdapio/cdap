package com.continuuity.passport.dal;


import com.continuuity.passport.common.sql.SQLChainImpl;
import com.continuuity.passport.common.sql.SQLChain;
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


  public boolean insertSingleInAccount(Connection connection,String email, String name) throws SQLException {

    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.insert("account").columns("name,email_id").
      values(name, email).execute();

  }

  List<Map<String,Object>> selectAllFromAccount(Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.select("account").includeAll().noWhere().execute();
  }

  List<Map<String,Object>> selectNone(Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.select("account").includeAll().where("email_id").equal("foo@bar.com").execute();
  }

  List<Map<String,Object>> selectNameFromAccount(Connection connection, String name) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.select("account").includeAll().where("name").equal(name).execute();
  }


  List<Map<String,Object>> selectEmail(Connection connection, String email) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.select("account").includeAll().where("email_id").equal(email).execute();
  }



  public boolean deleteOneFromAccount(Connection connection, String email) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.delete("ACCOUNT").where("email_id").equal(email).execute();

  }

  public boolean deleteAllFromAccount(Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.delete("ACCOUNT").noWhere().execute();

  }

  public boolean updateManyColumnsInAccount (Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.update("account").set("company","Continuuity").set("email_id","sree@continuuity.com")
                .setLast("name","Sree").where("email_id").equal("sree@gmail.com").execute();
  }

  public boolean updateOneInAccount(Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.update("account").setLast("name","Sreevatsan Raman").where("name").equal("sree").execute();
  }

  public boolean insertOneIntoVPC( Connection connection, String name, String vpcName) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.insert("vpc").columns("account_name","vpc_name").values(name,vpcName).execute();
  }

  List<Map<String,Object>> selectFromVPC(Connection connection, String name) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
    return chain.select("vpc").includeAll().where("account_name").equal(name).execute();
  }

  List<Map<String,Object>> selectJoin(Connection connection) throws SQLException {
    SQLChain chain = SQLChainImpl.getSqlChain(connection);
   // return chain.select("vpc").includeAll().where("name").equal(name).execute();
   return chain.selectWithJoin("account","vpc").joinOn().condition("account.name = vpc.account_name")
                                               .where("account.name").equal("sree").execute();
  }


  @Test
  public void testInsertAndSelect() throws SQLException, ClassNotFoundException {


    //Startup HSQL instance
    TestHelper.startHsqlDB();
    Connection connection =DriverManager.getConnection("jdbc:hsqldb:mem:test;" +
                             "hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false", "sa", "");


    //Insert
    //PreparedStatment's execute returns false on inserts
    assertFalse(insertSingleInAccount(connection, "sree@gmail.com", "sree"));

    //Select all and verify insert
    assertEquals(1,selectAllFromAccount(connection).size() );

    //Insert into VPC table;
    assertFalse(insertOneIntoVPC(connection,"sree","vpc1"));

    //Select from VPC and veriofy
    assertEquals(1,selectFromVPC(connection,"sree").size());

    //Check Join
    List<Map<String,Object>> data = selectJoin(connection);
    assertEquals(1,data.size());

    for( Map<String,Object> d : data) {
      assertEquals("sree",d.get("NAME"));
      assertEquals("vpc1",d.get("VPC_NAME"));
    }


    //Select none
    assertEquals(0, selectNone(connection).size());

    //Insert again
    assertFalse(insertSingleInAccount(connection,"simpson@homer.com","homer simpson"));

    //Select by name
    assertEquals(1,selectNameFromAccount(connection,"homer simpson").size());

    //Select all count 2
    assertEquals(2, selectAllFromAccount(connection).size());

    //Delete one
    assertFalse(deleteOneFromAccount(connection, "simpson@homer.com"));

    //Verify the data is deleted
    assertEquals(1, selectAllFromAccount(connection).size());


    //Update an entry
    assertFalse(updateOneInAccount(connection));

    //Verify Updates
    data  = selectEmail(connection,"sree@gmail.com");
    for  (Map<String,Object> d : data){
      assertEquals("Sreevatsan Raman",d.get("NAME"));
    }

    //Update multiple Entries
    assertFalse(updateManyColumnsInAccount(connection));

    //Verify result of updating multiple columns
     data  = selectEmail(connection,"sree@continuuity.com");
    for  (Map<String,Object> d : data){
      assertEquals("Sree",d.get("NAME"));
      assertEquals("Continuuity",d.get("COMPANY"));
    }

    //Delete all
    assertFalse(deleteAllFromAccount(connection));

    //Verify if delete all worked
    assertEquals(0,selectAllFromAccount(connection).size());

    //Stop HSQL instance
    TestHelper.stopHsqlDB();

  }

}
