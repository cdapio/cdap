/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.Constants;
import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.utils.ApiKey;
import com.continuuity.passport.core.utils.PasswordUtils;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.BillingInfo;
import com.continuuity.passport.meta.Role;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Map;

/**
 * AccountDAO implementation that uses mysql as the persistence store
 */
public class AccountDBAccess extends DBAccess implements AccountDAO {
  private DBConnectionPoolManager poolManager = null;
  private final String DB_INTEGRITY_CONSTRAINT_VIOLATION = "23000";
  private final HashFunction hashFunction = Hashing.sha1();

  /**
   * Guice injected AccountDBAccess. The parameters needed for DB will be injected as well.
   */
  @Inject
  public void AccountDBAccess(@Named("passport.config") Map<String, String> config) {
    String connectionString = config.get(Constants.CFG_JDBC_CONNECTION_STRING);
    String jdbcType = config.get(Constants.CFG_JDBC_TYPE);

    if (jdbcType.toLowerCase().equals(Constants.DEFAULT_JDBC_TYPE)) {
      MysqlConnectionPoolDataSource mysqlDataSource = new MysqlConnectionPoolDataSource();
      mysqlDataSource.setUrl(connectionString);
      this.poolManager = new DBConnectionPoolManager(mysqlDataSource, 20);
    }
  }

  /**
   * Create Account in the system
   *
   * @param account Instance of {@code Account}
   * @return boolean status of account creation
   * @throws {@code RetryException}
   */
  @Override
  public Account createAccount(Account account) throws ConfigurationException, AccountAlreadyExistsException {
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet result = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES (?,?,?,?,?,?)",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.EMAIL_COLUMN, DBUtils.AccountTable.FIRST_NAME_COLUMN,
        DBUtils.AccountTable.LAST_NAME_COLUMN, DBUtils.AccountTable.COMPANY_COLUMN,
        DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.ACCOUNT_CREATED_AT
      );

      ps = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
      ps.setString(1, account.getEmailId());
      ps.setString(2, account.getFirstName());
      ps.setString(3, account.getLastName());
      ps.setString(4, account.getCompany());
      ps.setInt(5, DBUtils.AccountTable.ACCOUNT_UNCONFIRMED);
      ps.setTimestamp(6, new java.sql.Timestamp(System.currentTimeMillis()));

      ps.executeUpdate();

      result = ps.getGeneratedKeys();
      if (result == null) {
        throw new RuntimeException("Failed Insert");
      }
      result.next();

      Account createdAccount = new Account(account.getFirstName(), account.getLastName(),
        account.getCompany(), account.getEmailId(), result.getInt(1));
      return createdAccount;
    } catch (SQLException e) {
      if (DB_INTEGRITY_CONSTRAINT_VIOLATION.equals(e.getSQLState())) {
        throw new AccountAlreadyExistsException(e.getMessage());
      }

      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    } finally {
      close(connection, ps, result);
    }
  }

  public boolean confirmRegistration(Account account, String password)
    throws ConfigurationException, RuntimeException {

    Connection connection = null;
    PreparedStatement updateStatement = null;
    PreparedStatement deleteStatement = null;

    Preconditions.checkNotNull(this.poolManager,"Data connection manager is null. Cannot connect to data source");

    String UPDATE_SQL = String.format("UPDATE %s SET %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ? WHERE %s = ?",
      DBUtils.AccountTable.TABLE_NAME,
      DBUtils.AccountTable.PASSWORD_COLUMN, DBUtils.AccountTable.CONFIRMED_COLUMN,
      DBUtils.AccountTable.API_KEY_COLUMN, DBUtils.AccountTable.FIRST_NAME_COLUMN,
      DBUtils.AccountTable.LAST_NAME_COLUMN, DBUtils.AccountTable.COMPANY_COLUMN,
      DBUtils.AccountTable.ID_COLUMN);

    String DELETE_NONCE_BY_EMAIL = String.format("DELETE FROM %s where %s = ?",
      DBUtils.Nonce.TABLE_NAME,DBUtils.Nonce.ID_COLUMN);

    try {
      connection = this.poolManager.getValidConnection();
      // Set Auto commit to false since the update and delete nonce should be one transaction
      connection.setAutoCommit(false);

      updateStatement = connection.prepareStatement(UPDATE_SQL);
      updateStatement.setString(1, PasswordUtils.generateHashedPassword(password));
      updateStatement.setInt(2, DBUtils.AccountTable.ACCOUNT_CONFIRMED);
      updateStatement.setString(3, ApiKey.generateKey(String.valueOf(account.getAccountId())));
      updateStatement.setString(4, account.getFirstName());
      updateStatement.setString(5, account.getLastName());
      updateStatement.setString(6, account.getCompany());
      updateStatement.setInt(7, account.getAccountId());

      updateStatement.executeUpdate();

      deleteStatement = connection.prepareStatement(DELETE_NONCE_BY_EMAIL);
      deleteStatement.setString(1,account.getEmailId());
      deleteStatement.executeUpdate();

      //Commit the transaction
      connection.commit();

    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    } finally {
      close(null,deleteStatement);
      close(connection, updateStatement);
    }

    return true;
  }

  /**
   * @param accountId
   * @throws ConfigurationException
   */
  @Override
  public void confirmDownload(int accountId) throws ConfigurationException {
    Connection connection = null;
    PreparedStatement ps = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("UPDATE %s SET %s = ? WHERE %s = ? AND %s is NULL",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
        DBUtils.AccountTable.ID_COLUMN,
        DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT);

      ps = connection.prepareStatement(SQL);

      ps.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
      ps.setInt(2, accountId);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }
  }

  /**
   * Delete Account in the system
   *
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion
   * @throws {@code RetryException}
   */
  @Override
  public boolean deleteAccount(int accountId)
    throws ConfigurationException, AccountNotFoundException {

    PreparedStatement ps = null;
    Connection connection = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("DELETE FROM %s WHERE %s = ?",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.ID_COLUMN);
      ps = connection.prepareStatement(SQL);

      ps.setInt(1, accountId);
      int affectedRows = ps.executeUpdate();
      if (affectedRows == 0) {
        throw new AccountNotFoundException("Account doesn't exists");
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
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
  public Account getAccount(int accountId) throws ConfigurationException {

    Account account = null;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();

      String SQL = String.format("SELECT %s,%s,%s,%s,%s,%s,%s,%s FROM %s WHERE %s = ?",
        DBUtils.AccountTable.FIRST_NAME_COLUMN, DBUtils.AccountTable.LAST_NAME_COLUMN,
        DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
        DBUtils.AccountTable.ID_COLUMN, DBUtils.AccountTable.API_KEY_COLUMN,
        DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.ID_COLUMN);

      ps = connection.prepareStatement(SQL);
      ps.setInt(1, accountId);
      rs = ps.executeQuery();

      int count = 0;
      while (rs.next()) {
        count++;
        account = new Account(rs.getString(1), rs.getString(2), rs.getString(3),
          rs.getString(4), rs.getInt(5), rs.getString(6),
          rs.getBoolean(7), DBUtils.getDevsuiteDownloadedTime(rs.getTimestamp(8)));
        if (count > 1) { // Note: This condition should never occur since ids are auto generated.
          throw new RuntimeException("Multiple accounts with same account ID");
        }
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps, rs);
    }
    return account;
  }

  /**
   * GetAccount
   *
   * @param emailId emailId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  @Override
  public Account getAccount(String emailId) throws ConfigurationException {

    Account account = null;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();

      String SQL = String.format("SELECT %s,%s,%s,%s,%s,%s,%s,%s FROM %s WHERE %s = ?",
        DBUtils.AccountTable.FIRST_NAME_COLUMN, DBUtils.AccountTable.LAST_NAME_COLUMN,
        DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
        DBUtils.AccountTable.ID_COLUMN, DBUtils.AccountTable.API_KEY_COLUMN,
        DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.EMAIL_COLUMN);

      ps = connection.prepareStatement(SQL);
      ps.setString(1, emailId);
      rs = ps.executeQuery();

      int count = 0;
      while (rs.next()) {
        count++;
        account = new Account(rs.getString(1), rs.getString(2), rs.getString(3),
          rs.getString(4), rs.getInt(5), rs.getString(6),
          rs.getBoolean(7), DBUtils.getDevsuiteDownloadedTime(rs.getTimestamp(8)));
        if (count > 1) { // Note: This condition should never occur since ids are auto generated.
          throw new RuntimeException("Multiple accounts with same account ID");
        }
      }

    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps, rs);
    }


    return account;
  }


  @Override
  public boolean updateBillingInfo(int accountId, BillingInfo billingInfo) throws ConfigurationException {

    Connection connection = null;
    PreparedStatement ps = null;

    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();


      String SQL = String.format("INSERT INTO %s (%s,%s,%s,%s,%s) VALUES(?,?,?,?,?)",
        DBUtils.AccountPayment.TABLE_NAME,
        DBUtils.AccountPayment.ACCOUNT_ID_COLUMN,
        DBUtils.AccountPayment.CREDIT_CARD_NAME_COLUMN,
        DBUtils.AccountPayment.CREDIT_CARD_NUMBER_COLUMN,
        DBUtils.AccountPayment.CREDIT_CARD_CVV_COLUMN,
        DBUtils.AccountPayment.CREDIT_CARD_EXPIRY_COLUMN);

      ps = connection.prepareStatement(SQL);

      ps.setInt(1, accountId);
      ps.setString(2, billingInfo.getCreditCardName());
      ps.setString(3, billingInfo.getCreditCardNumber());
      ps.setString(4, billingInfo.getCvv());
      ps.setString(5, billingInfo.getExpirationDate());

      ps.executeUpdate();

    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }

    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }


  @Override
  public boolean addRoleType(int accountId, Role role) throws ConfigurationException {

    Connection connection = null;
    PreparedStatement ps = null;

    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("INSERT INTO %s (%s,%s,%s,%s,%s) VALUES(?,?,?,?,?)",
        DBUtils.AccountRoleType.TABLE_NAME,
        DBUtils.AccountRoleType.ACCOUNT_ID_COLUMN,
        DBUtils.AccountRoleType.ROLE_NAME_COLUMN,
        DBUtils.AccountRoleType.PERMISSIONS_COLUMN);

      ps = connection.prepareStatement(SQL);

      ps.setInt(1, accountId);
      ps.setString(2, role.getRoleName());
      ps.setString(3, role.getPermissions());
      ps.executeUpdate();

    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }

    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void updateAccount(int accountId, Map<String, Object> keyValueParams) throws ConfigurationException {
    Connection connection = null;
    PreparedStatement ps = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {

      connection = this.poolManager.getValidConnection();

      //Set basic update command
      StringBuilder sb = new StringBuilder();

      sb.append(String.format("UPDATE %s SET ", DBUtils.AccountTable.TABLE_NAME));
      boolean firstValue = true;

      if (!keyValueParams.isEmpty()) {

        //Add Column names
        for (Map.Entry e : keyValueParams.entrySet()) {

          if (firstValue) {
            sb.append(String.format(" %s= ?", (String) e.getKey()));
            firstValue = false;
          } else {
            //append a comma as well
            sb.append(String.format(", %s = ?", (String) e.getKey()));
          }
        }

        sb.append(String.format(" where %s = ? ", DBUtils.AccountTable.ID_COLUMN));

        //Prepared Statement
        ps = connection.prepareStatement(sb.toString());
        int count = 1;

        //Set Values in prepared statement
        //All values are set as String for now.
        //For now we are only updating String fields
        // TODO: Enhance it to actual type of columns later.

        for (Map.Entry e : keyValueParams.entrySet()) {
          ps.setString(count, (String) e.getValue());
          count++;
        }

        //Set value for where clause
        ps.setInt(count, accountId);
        ps.executeUpdate();

      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }

  }

  @Override
  public void changePassword(int accountId, String oldPassword, String newPassword) {
    Connection connection = null;
    PreparedStatement ps = null;

    try {
      connection = this.poolManager.getValidConnection();

      String SQL = String.format("UPDATE %s SET %s = ? WHERE %s = ? AND %s = ?",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.PASSWORD_COLUMN,
        DBUtils.AccountTable.ID_COLUMN,
        DBUtils.AccountTable.PASSWORD_COLUMN);

      ps = connection.prepareStatement(SQL);
      ps.setString(1, PasswordUtils.generateHashedPassword(newPassword));
      ps.setInt(2, accountId);
      ps.setString(3, PasswordUtils.generateHashedPassword(oldPassword));
      int count = ps.executeUpdate();
      Preconditions.checkArgument(count==1,"Update password failed");
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }
  }

  @Override
  public Account resetPassword(int nonce, String newPassword) {

    //NOTE: This code does two things
    // 1) Update the Password getting the email id from nonce table
    // 2) Fetch Account information joining with nonce table to email id.

    Account account = null;
    Connection connection = null;
    PreparedStatement update = null;
    PreparedStatement select = null;
    PreparedStatement delete = null;

    ResultSet rs = null;

    //Update the account table. Joining email id from nonce table
    String UPDATE_SQL = String.format("UPDATE %s set %s = ? where %s = (SELECT %s from %s where %s = ?)",
      DBUtils.AccountTable.TABLE_NAME,
      DBUtils.AccountTable.PASSWORD_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
      DBUtils.Nonce.ID_COLUMN, DBUtils.Nonce.TABLE_NAME, DBUtils.Nonce.NONCE_ID_COLUMN
    );

    //Update is successful. now return the account information
    String SELECT_SQL = String.format("SELECT %s,%s,%s,%s,%s,%s,%s,%s FROM %s WHERE %s = " +
      "(SELECT %s FROM %s where %s = ?)",
      DBUtils.AccountTable.FIRST_NAME_COLUMN, DBUtils.AccountTable.LAST_NAME_COLUMN,
      DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
      DBUtils.AccountTable.ID_COLUMN, DBUtils.AccountTable.API_KEY_COLUMN,
      DBUtils.AccountTable.CONFIRMED_COLUMN,
      DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
      DBUtils.AccountTable.TABLE_NAME,
      DBUtils.AccountTable.EMAIL_COLUMN,
      DBUtils.Nonce.ID_COLUMN, DBUtils.Nonce.TABLE_NAME,
      DBUtils.Nonce.NONCE_ID_COLUMN
    );

    String DELETE_NONCE = String.format("DELETE FROM %s where %s = ?",
      DBUtils.Nonce.TABLE_NAME,DBUtils.Nonce.NONCE_ID_COLUMN);

    try {
      connection = this.poolManager.getValidConnection();
      connection.setAutoCommit(false);

      update = connection.prepareStatement(UPDATE_SQL);
      update.setString(1, PasswordUtils.generateHashedPassword(newPassword));
      update.setInt(2, nonce);
      update.executeUpdate();



      select = connection.prepareStatement(SELECT_SQL);

      select.setInt(1, nonce);
      rs = select.executeQuery();

      delete = connection.prepareStatement(DELETE_NONCE);
      delete.setInt(1,nonce);
      delete.executeUpdate();

      connection.commit();

      while (rs.next()) {
        account = new Account(rs.getString(1), rs.getString(2), rs.getString(3),
          rs.getString(4), rs.getInt(5), rs.getString(6),
          rs.getBoolean(7), DBUtils.getDevsuiteDownloadedTime(rs.getTimestamp(8)));
      }
      return account;
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(null,update);
      close(null,delete);
      close(connection, select, rs);
    }
  }

  @Override
  public void regenerateApiKey(int accountId) {
    Connection connection = null;
    PreparedStatement ps = null;
    Preconditions.checkNotNull(this.poolManager, "DBConnection pool is null. DAO is not configured");

    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("UPDATE %s SET %s = ? WHERE %s = ?",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.API_KEY_COLUMN,
        DBUtils.AccountTable.ID_COLUMN);

      ps = connection.prepareStatement(SQL);
      ps.setString(1, ApiKey.generateKey(String.valueOf(accountId)));
      ps.setInt(2, accountId);

      ps.executeUpdate();

    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    } finally {
      close(connection, ps);
    }
  }

}
