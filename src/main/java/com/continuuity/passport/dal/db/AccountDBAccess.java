package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.common.sql.SQLChain;
import com.continuuity.passport.common.sql.SQLChainImpl;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.meta.AccountSecurity;
import com.continuuity.passport.core.meta.BillingInfo;
import com.continuuity.passport.core.meta.Role;
import com.continuuity.passport.dal.AccountDAO;
import  java.sql.Connection;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import java.sql.SQLException;
import java.util.List;
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
  public boolean createAccount(Account account) throws ConfigurationException, RuntimeException {
    //TODO: Return boolean?
    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection= this.poolManager.getConnection();
      //TODO: Execute in a thread ...
      SQLChain chain =  SQLChainImpl.getSqlChain(connection);
      chain.insert(Common.AccountTable.TABLE_NAME)
           .columns(Common.AccountTable.EMAIL_COLUMN, Common.AccountTable.NAME_COLUMN,
                    Common.AccountTable.CONFIRMED_COLUMN)
           .values(account.getEmailId(), account.getName(), Common.AccountTable.ACCOUNT_UNCONFIRMED)
           .execute();
    } catch (SQLException e) {
      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
    return true;
  }


  public boolean confirmRegistration(AccountSecurity security) throws ConfigurationException, RuntimeException{

    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection = this.poolManager.getConnection();
      SQLChain chain = SQLChainImpl.getSqlChain(connection);
      //TODO: Update count should be 1
      chain.update(Common.AccountTable.TABLE_NAME)
           .set(Common.AccountTable.PASSWORD_COLUMN, generateSaltedHashedPassword(security.getPassword()))
           .set(Common.AccountTable.CONFIRMED_COLUMN, Common.AccountTable.ACCOUNT_CONFIRMED)
           .setLast(Common.AccountTable.API_KEY_COLUMN, generateAPIKey())
           .where(Common.AccountTable.EMAIL_COLUMN).equal(security.getAccount().getEmailId()).execute();
    }
    catch (SQLException e){
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

    if(this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection = this.poolManager.getConnection();
      SQLChain chain = SQLChainImpl.getSqlChain(connection);
      chain.delete(Common.AccountTable.TABLE_NAME)
           .where(Common.AccountTable.EMAIL_COLUMN).equal(accountId).execute();

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
      System.out.println("In get account DB Access");
      Connection connection = this.poolManager.getConnection();
      System.out.println("Got connection");
      if (connection == null) {System.out.println("Empty Connection");}
      SQLChain chain = SQLChainImpl.getSqlChain(connection);
      System.out.println(connection.toString());

      List<Map<String,Object>> resultSet = chain.select(Common.AccountTable.TABLE_NAME)
                                                .include(Common.AccountTable.ID_COLUMN,
                                                         Common.AccountTable.EMAIL_COLUMN,
                                                         Common.AccountTable.NAME_COLUMN)
                                                .where(Common.AccountTable.ID_COLUMN).equal(accountId)
                                                .execute();


      System.out.println("Result Size: "+resultSet.size() );
       if (resultSet.size() == 1 ) {

         Map<String,Object> dataSet = resultSet.get(0);
         account = new Account((String)dataSet.get(Common.AccountTable.NAME_COLUMN.toLowerCase()),
                               (String)dataSet.get(Common.AccountTable.EMAIL_COLUMN.toLowerCase()),
                               (Integer)dataSet.get(Common.AccountTable.ID_COLUMN.toLowerCase()));

       }


    }
    catch (SQLException e) {
      System.out.println(e.getMessage());
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
      SQLChain chain = SQLChainImpl.getSqlChain(connection);
      chain.insert(Common.AccountPayment.TABLE_NAME)
           .columns(Common.AccountPayment.ACCOUNT_ID_COLUMN, Common.AccountPayment.CREDIT_CARD_NAME_COLUMN,
                    Common.AccountPayment.CREDIT_CARD_NUMBER_COLUMN,Common.AccountPayment.CREDIT_CARD_CVV_COLUMN,
                    Common.AccountPayment.CREDIT_CARD_EXPIRY_COLUMN)
           .values(accountId,billingInfo.getCreditCardName(),billingInfo.getCreditCardNumber(),
                   billingInfo.getCvv(),billingInfo.getExpirationDate())
           .execute();
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

      SQLChain chain = SQLChainImpl.getSqlChain(connection);
      chain.insert(Common.AccountRoleType.TABLE_NAME)
           .columns(Common.AccountRoleType.ACCOUNT_ID_COLUMN,Common.AccountRoleType.ROLE_NAME_COLUMN,
                    Common.AccountRoleType.PERMISSIONS_COLUMN)
           .values(accountId, role.getRoleName(),role.getPermissions())
           .execute();
    }
    catch (SQLException e) {
      throw new RuntimeException(e.getMessage(),e.getCause());
    }
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  private String generateAPIKey(){
    //TODO: Generate API_KEY
    return "API_KEY";
  }

  private String generateSaltedHashedPassword(String password) {
    //TODO: Add this
    return password;

  }

}
