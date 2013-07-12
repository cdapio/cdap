package com.continuuity.passport.testhelper;

import org.hsqldb.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * Test Helper for unit/integration tests.
 * Uses HSQLDB instance for testing.
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
                                                     "org_id VARCHAR(100) DEFAULT null, " +
                                                     "UNIQUE (email_id), " +
                                                     "FOREIGN KEY(org_id) " +
                                                     "REFERENCES organization(id)" +
                                                     ")";

  private static final String CREATE_VPC_ACCOUNT_TABLE = "CREATE TABLE vpc_account ( id INTEGER IDENTITY, " +
                                                         "account_id INTEGER, vpc_name VARCHAR(100), " +
                                                         "vpc_created_at TIMESTAMP, vpc_label VARCHAR(100), " +
                                                         "vpc_type VARCHAR(30) " +
                                                         ")";

  private static final String CREATE_VPC_ROLE_TABLE = "CREATE TABLE vpc_roles ( vpc_id INTEGER , " +
    "account_id INTEGER, role_type INTEGER, role_overrides VARCHAR(100) )";


  private static final String CREATE_ORG_TABLE = "CREATE TABLE organization (id VARCHAR(100) PRIMARY KEY, " +
                                                 "name VARCHAR(100))";

  private static final String CREATE_NONCE_TABLE = "CREATE TABLE nonce (nonce_id INTEGER IDENTITY," +
                                                   "id VARCHAR(100), nonce_expires_at TIMESTAMP, UNIQUE (id)" +
                                                   ")";
  private static final String DROP_ACCOUNT_TABLE = "DROP TABLE account";
  private static final String DROP_NONCE_TABLE = "DROP TABLE nonce";
  private static final String DROP_VPC_ACCOUNT_TABLE = "DROP TABLE vpc_account";
  private static final String DROP_VPC_ROLE_TABLE = "DROP TABLE vpc_roles";
  private static final String DROP_ORG_TABLE = "DROP TABLE organization";




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

    connection.createStatement().execute(CREATE_ORG_TABLE);
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
    connection.createStatement().execute(DROP_ORG_TABLE);
    connection.close();

    server.stop();
  }


  public static void insertIntoVPCRoleTable(int vpcId, int accountId) throws SQLException {
    String sql = String.format("INSERT INTO vpc_roles (vpc_id, account_id) VALUES (?, ?)", vpcId, accountId);
    PreparedStatement ps = connection.prepareStatement(sql);
    ps.setInt(1, vpcId);
    ps.setInt(2, accountId);
    ps.execute();
  }
}
