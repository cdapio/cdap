package com.continuuity.passport.testhelper;

import org.hsqldb.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * Test Helper for unit/integration tests
 * Uses HSQLDB instance for testing
 */
public class HyperSQL {

  private static Server server = null;

  protected static Connection connection;

  private static final String CREATE_ACCOUNT_TABLE = "CREATE TABLE account (id INTEGER IDENTITY , " +
                                                     "first_name VARCHAR(50),last_name VARCHAR(50), " +
                                                     "company VARCHAR(50),email_id VARCHAR(50), " +
                                                     "password VARCHAR(100),confirmed INTEGER, " +
                                                     "api_key VARCHAR(100),account_created_at DATETIME," +
                                                     "dev_suite_downloaded_at TIMESTAMP DEFAULT null," +
                                                     "payment_account_id VARCHAR(30) DEFAULT null,"   +
                                                     "payment_info_provided_at TIMESTAMP DEFAULT null," +
                                                     "UNIQUE (email_id)" +
                                                     ")";

  private static final String CREATE_VPC_ACCOUNT_TABLE = "CREATE TABLE vpc_account ( id INTEGER IDENTITY, " +
                                                         "account_id INTEGER, vpc_name VARCHAR(100), " +
                                                         "vpc_created_at TIMESTAMP, vpc_label VARCHAR(100), " +
                                                         "vpc_type VARCHAR(30) " +
                                                         ")" ;

  private static final String CREATE_VPC_ROLE_TABLE = "CREATE TABLE vpc_role ( vpc_id INTEGER , " +
    "account_id INTEGER, role_type INTEGER, role_overrides VARCHAR(100) )" ;



  private static final String CREATE_NONCE_TABLE = "CREATE TABLE nonce (nonce_id INTEGER IDENTITY," +
                                                   "id VARCHAR(100), nonce_expires_at TIMESTAMP, UNIQUE (id)" +
                                                   ")";
  private static final String DROP_ACCOUNT_TABLE = "DROP TABLE account";
  private static final String DROP_NONCE_TABLE = "DROP TABLE nonce";
  private static final String DROP_VPC_ACCOUNT_TABLE = "DROP TABLE vpc_account";
  private static final String DROP_VPC_ROLE_TABLE = "DROP TABLE vpc_role";




  public static void startHsqlDB() throws SQLException, ClassNotFoundException {

    System.out.println("======================================START======================================");

    server = new Server();
    server.setLogWriter(null);
    server.setPort(1234);
    server.setSilent(true);

    server.setDatabaseName(0, "xdb");
    server.setDatabasePath(0, "mem:test");

    server.start();
    Class.forName("org.hsqldb.jdbcDriver");
    connection = DriverManager.getConnection("jdbc:hsqldb:mem:test;" +
      "hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false", "sa", "");


    connection.createStatement().execute(CREATE_ACCOUNT_TABLE);
    connection.createStatement().execute(CREATE_NONCE_TABLE);
    connection.createStatement().execute(CREATE_VPC_ACCOUNT_TABLE);
    connection.createStatement().execute(CREATE_VPC_ROLE_TABLE);
  }


  public static void stopHsqlDB() throws SQLException {

    System.out.println("======================================STOP=======================================");
    connection.createStatement().execute(DROP_ACCOUNT_TABLE);
    connection.createStatement().execute(DROP_NONCE_TABLE);
    connection.createStatement().execute(DROP_VPC_ACCOUNT_TABLE);
    connection.createStatement().execute(DROP_VPC_ROLE_TABLE);

    connection.close();
    server.stop();
  }

}
