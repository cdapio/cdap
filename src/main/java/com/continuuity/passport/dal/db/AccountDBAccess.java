package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.AccountSecurity;
import com.continuuity.passport.core.meta.BillingInfo;
import com.continuuity.passport.core.meta.Role;
import com.continuuity.passport.core.utils.ApiKey;
import com.continuuity.passport.dal.AccountDAO;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Map;

/**
 * AccountDAO implementation that uses database as the persistence store
 */
public class AccountDBAccess implements AccountDAO {

  private Map<String, String> configuration;

  private DBConnectionPoolManager poolManager = null;

  /**
   * Create Account in the system
   * @param account Instance of {@code Account}
   * @return boolean status of account creation
   * @throws {@code RetryException}
   */
  @Override
  public long createAccount(Account account) throws ConfigurationException, RuntimeException {
    //TODO: Return boolean?
    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection= this.poolManager.getConnection();
      String SQL = String.format( "INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES (?,?,?,?,?,?)",
                                  DBUtils.AccountTable.TABLE_NAME,
                                  DBUtils.AccountTable.EMAIL_COLUMN, DBUtils.AccountTable.FIRST_NAME_COLUMN,
                                  DBUtils.AccountTable.LAST_NAME_COLUMN, DBUtils.AccountTable.COMPANY_COLUMN,
                                  DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.ACCOUNT_CREATED_AT
                                  );


      Date date = new Date();
      PreparedStatement ps = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
      ps.setString(1, account.getEmailId());
      ps.setString(2,account.getFirstName());
      ps.setString(3,account.getLastName());
      ps.setString(4, account.getCompany());
      ps.setInt(5, DBUtils.AccountTable.ACCOUNT_UNCONFIRMED);
      ps.setTimestamp(6, new java.sql.Timestamp(date.getTime()));
      ps.executeUpdate();

      ResultSet result = ps.getGeneratedKeys();
      if (result == null) {
        throw new RuntimeException("Failed Insert");
      }
      result.next();
      long id = result.getLong(1);
      return id;
    } catch (SQLException e) {
      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }


  public boolean confirmRegistration(AccountSecurity security) throws ConfigurationException, RuntimeException{

    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection = this.poolManager.getConnection();
      String SQL = String.format( "UPDATE %s SET %s = ?, %s = ?, %s = ? WHERE %s = ?" ,
                                          DBUtils.AccountTable.TABLE_NAME,
                                          DBUtils.AccountTable.PASSWORD_COLUMN, DBUtils.AccountTable.CONFIRMED_COLUMN,
                                          DBUtils.AccountTable.API_KEY_COLUMN, DBUtils.AccountTable.ID_COLUMN);

      PreparedStatement ps = connection.prepareStatement(SQL);
      ps.setString(1, generateSaltedHashedPassword(security.getPassword()));
      ps.setInt(2, DBUtils.AccountTable.ACCOUNT_CONFIRMED);
      ps.setString(3, ApiKey.generateKey(String.valueOf(security.getAccountId())));
      ps.setInt(4, security.getAccountId());

      ps.executeUpdate();

    }
    catch (SQLException e){
      throw new RuntimeException(e.getMessage(),e.getCause());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage(),e.getCause());
    }

    return true;
  }

  /**
   * Delete Account in the system
   *
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion
   * @throws {@code RetryException}
   */
  @Override
  public boolean deleteAccount(String accountId) throws ConfigurationException, RuntimeException {

    //TODO: accountId to int
    if(this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection = this.poolManager.getConnection();
      String SQL = String.format( "DELETE FROM %s WHERE %s = ?",
                                  DBUtils.AccountTable.TABLE_NAME,
                                  DBUtils.AccountTable.ID_COLUMN);
      PreparedStatement ps = connection.prepareStatement(SQL);

      ps.setString(1, accountId);
      ps.executeUpdate();

    }
    catch (SQLException e){
      throw new RuntimeException(e.getMessage(),e.getCause());
    }
    return true;
  }

  /**
   * GetAccount
   *
   * @param accountId id of the account
   * @return null if no entry matches, Instance of {@code Account} otherwise
   * @throws {@code RetryException}
   */
  @Override
  public Account getAccount(int accountId) throws ConfigurationException, RuntimeException {

    Account account = null;

    if(this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection = this.poolManager.getConnection();

      String SQL = String.format( "SELECT %s,%s,%s,%s,%s, %s FROM %s WHERE %s = ?",
                                  DBUtils.AccountTable.FIRST_NAME_COLUMN,DBUtils.AccountTable.LAST_NAME_COLUMN,
                                  DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
                                  DBUtils.AccountTable.ID_COLUMN, DBUtils.AccountTable.API_KEY_COLUMN,
                                  DBUtils.AccountTable.TABLE_NAME,
                                  DBUtils.AccountTable.ID_COLUMN);

      PreparedStatement ps = connection.prepareStatement(SQL);
      ps.setInt(1,accountId);
      ResultSet rs = ps.executeQuery();

      int count  = 0;
      while(rs.next()) {
        count++;
        account = new Account(rs.getString(1),rs.getString(2),rs.getString(3),
                              rs.getString(4),rs.getInt(5),rs.getString(6));
        if (count > 1 ) { // Note: This condition should never occur since ids are auto generated.
          throw new RuntimeException("Multiple accounts with same account ID");
        }
      }

    }
    catch (SQLException e) {
      throw new RuntimeException(e.getMessage(),e.getCause());
    }
    return account;
  }


  @Override
  public boolean updateBillingInfo(int accountId, BillingInfo billingInfo) throws ConfigurationException,RuntimeException {
    if(this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection = this.poolManager.getConnection();


      String SQL = String.format( "INSERT INTO %s (%s,%s,%s,%s,%s) VALUES(?,?,?,?,?)" ,
                                    DBUtils.AccountPayment.TABLE_NAME,
                                    DBUtils.AccountPayment.ACCOUNT_ID_COLUMN,
                                    DBUtils.AccountPayment.CREDIT_CARD_NAME_COLUMN,
                                    DBUtils.AccountPayment.CREDIT_CARD_NUMBER_COLUMN,
                                    DBUtils.AccountPayment.CREDIT_CARD_CVV_COLUMN,
                                    DBUtils.AccountPayment.CREDIT_CARD_EXPIRY_COLUMN);

      PreparedStatement ps = connection.prepareStatement(SQL);

      ps.setInt(1,accountId);
      ps.setString(2, billingInfo.getCreditCardName());
      ps.setString(3, billingInfo.getCreditCardNumber());
      ps.setString(4,billingInfo.getCvv());
      ps.setString(5,billingInfo.getExpirationDate());

      ps.executeUpdate();

    }
    catch (SQLException e){
      throw new RuntimeException(e.getMessage(),e.getCause());
    }

    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }


  /**
   * Configure the Data access objects. Creates a connection pool manager
   * @param configuration Key value params for configuring the DAO
   */
  @Override
  public void configure(Map<String, String> configuration) {

    this.configuration = configuration;
    String connectionString = configuration.get("connectionString");

    String jdbcType = configuration.get("jdbcType");

    if (jdbcType.toLowerCase().equals("mysql")) {

      MysqlConnectionPoolDataSource mysqlDataSource =  new MysqlConnectionPoolDataSource();
      mysqlDataSource.setUrl(connectionString);
      this.poolManager = new DBConnectionPoolManager(mysqlDataSource, 20);

    }
  }

  @Override
  public boolean addRoleType(int accountId, Role role) throws ConfigurationException, RuntimeException {
    if(this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection = this.poolManager.getConnection();
      String SQL = String.format( "INSERT INTO %s (%s,%s,%s,%s,%s) VALUES(?,?,?,?,?)" ,
                                              DBUtils.AccountRoleType.TABLE_NAME,
                                              DBUtils.AccountRoleType.ACCOUNT_ID_COLUMN,
                                              DBUtils.AccountRoleType.ROLE_NAME_COLUMN,
                                              DBUtils.AccountRoleType.PERMISSIONS_COLUMN);

      PreparedStatement ps = connection.prepareStatement(SQL);

      ps.setInt(1,accountId);
      ps.setString(2,role.getRoleName());
      ps.setString(3,role.getPermissions());
      ps.executeUpdate();

    }
    catch (SQLException e) {
      throw new RuntimeException(e.getMessage(),e.getCause());
    }
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void updateAccount(int accountId, Map<String, Object> keyValueParams) throws  ConfigurationException, RuntimeException {
    if(this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {

       Connection connection = this.poolManager.getConnection();

       //Set basic update command
       StringBuilder sb  = new StringBuilder();

       sb.append(String.format("UPDATE %s SET ",DBUtils.AccountTable.TABLE_NAME));
       boolean firstValue = true;

       if(!keyValueParams.isEmpty()){

         //Add Column names
         for(Map.Entry e: keyValueParams.entrySet()){

           if (firstValue){
             sb.append(String.format(" %s= ?", (String)e.getKey()));
             firstValue = false;
           }
           else {
             //append a comma as well
             sb.append(String.format(", %s = ?", (String) e.getKey()));
           }
         }

         sb.append(String.format(" where %s = ? ",DBUtils.AccountTable.ID_COLUMN));

         //Prepared Statement
         PreparedStatement ps = connection.prepareStatement(sb.toString());
         int count = 1;
         //Set Values in prepared statement
         //All values are set as String for now.
         //For now we are only updating String fields
         // TODO: Enhance it to actual type of columns later.

         for (Map.Entry e : keyValueParams.entrySet()){
           ps.setString(count,(String)e.getValue());
           count++;
         }

         //Set value for where clause
         ps.setInt(count,accountId);
         ps.executeUpdate();

       }
    }
    catch (SQLException e) {
      throw new RuntimeException(e.getMessage(),e.getCause());
    }
  }

  private String generateSaltedHashedPassword(String password) {
    //TODO: Add this
    return password;

  }

}
