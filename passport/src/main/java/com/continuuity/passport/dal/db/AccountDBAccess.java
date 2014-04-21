/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.OrganizationNotFoundException;
import com.continuuity.passport.core.utils.ApiKey;
import com.continuuity.passport.core.utils.PasswordUtils;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Role;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * AccountDAO implementation that uses mysql as the persistence store.
 */
public class AccountDBAccess extends DBAccess implements AccountDAO {
  private final DBConnectionPoolManager poolManager;
  /**
   * Guice injected AccountDBAccess. The parameters needed for DB will be injected as well.
   */
  @Inject
  public AccountDBAccess(DBConnectionPoolManager poolManager) {
    this.poolManager = poolManager;
  }

  @Override
  public Account createAccount(Account account) throws AccountAlreadyExistsException {
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet result = null;

    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES (?,?,?,?,?,?)",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.EMAIL_COLUMN, DBUtils.AccountTable.FIRST_NAME_COLUMN,
        DBUtils.AccountTable.LAST_NAME_COLUMN, DBUtils.AccountTable.COMPANY_COLUMN,
        DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.ACCOUNT_CREATED_AT
      );

      ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
      ps.setString(1, account.getEmailId());
      ps.setString(2, account.getFirstName());
      ps.setString(3, account.getLastName());
      ps.setString(4, account.getCompany());
      ps.setInt(5, DBUtils.AccountTable.ACCOUNT_UNCONFIRMED);
      ps.setTimestamp(6, new java.sql.Timestamp(System.currentTimeMillis()));

      ps.executeUpdate();
      connection.commit();

      result = ps.getGeneratedKeys();
      if (result == null) {
        throw new RuntimeException("Failed Insert");
      }
      result.next();

      Account createdAccount = new Account(account.getFirstName(), account.getLastName(),
        account.getCompany(), account.getEmailId(), result.getInt(1));
      return createdAccount;
    } catch (SQLException e) {
      if (DBUtils.DB_INTEGRITY_CONSTRAINT_VIOLATION.equals(e.getSQLState())) {
        throw new AccountAlreadyExistsException(e.getMessage());
      }
      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    } finally {
      close(connection, ps, result);
    }
  }

  public boolean confirmRegistration(Account account, String password) {

    Connection connection = null;
    PreparedStatement updateStatement = null;
    PreparedStatement deleteStatement = null;

    String updateSql = String.format("UPDATE %s SET %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ? WHERE %s = ?",
      DBUtils.AccountTable.TABLE_NAME,
      DBUtils.AccountTable.PASSWORD_COLUMN, DBUtils.AccountTable.CONFIRMED_COLUMN,
      DBUtils.AccountTable.API_KEY_COLUMN, DBUtils.AccountTable.FIRST_NAME_COLUMN,
      DBUtils.AccountTable.LAST_NAME_COLUMN, DBUtils.AccountTable.COMPANY_COLUMN,
      DBUtils.AccountTable.ID_COLUMN);

    String deleteNonceByEmail = String.format("DELETE FROM %s where %s = ?",
      DBUtils.Nonce.TABLE_NAME, DBUtils.Nonce.ID_COLUMN);

    try {
      connection = this.poolManager.getValidConnection();
      // Set Auto commit to false since the update and delete nonce should be one transaction
      connection.setAutoCommit(false);

      updateStatement = connection.prepareStatement(updateSql);
      updateStatement.setString(1, PasswordUtils.generateHashedPassword(password));
      updateStatement.setInt(2, DBUtils.AccountTable.ACCOUNT_CONFIRMED);
      updateStatement.setString(3, ApiKey.generateKey(String.valueOf(account.getAccountId())));
      updateStatement.setString(4, account.getFirstName());
      updateStatement.setString(5, account.getLastName());
      updateStatement.setString(6, account.getCompany());
      updateStatement.setInt(7, account.getAccountId());

      updateStatement.executeUpdate();

      deleteStatement = connection.prepareStatement(deleteNonceByEmail);
      deleteStatement.setString(1, account.getEmailId());
      deleteStatement.executeUpdate();

      //Commit the transaction
      connection.commit();

    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    } finally {
      close(null, deleteStatement);
      close(connection, updateStatement);
    }

    return true;
  }

  /**
   * @param accountId
   */
  @Override
  public void confirmDownload(int accountId) {
    Connection connection = null;
    PreparedStatement ps = null;

    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ? AND %s is NULL",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
        DBUtils.AccountTable.ID_COLUMN,
        DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT);

      ps = connection.prepareStatement(sql);

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
   * Confirm payment received and record first time payment is made.
   * @param accountId
   * @param paymentId id in the external system for payments
   */
  @Override
  public void confirmPayment(int accountId, String paymentId) {
    Connection connection = null;
    PreparedStatement ps = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("UPDATE %s SET %s = ?, %s = ? WHERE %s = ?", DBUtils.AccountTable.TABLE_NAME,
                                                                      DBUtils.AccountTable.PAYMENT_INFO_PROVIDED_AT,
                                                                      DBUtils.AccountTable.PAYMENT_ACCOUNT_ID,
                                                                      DBUtils.AccountTable.ID_COLUMN);
      ps = connection.prepareStatement(sql);
      ps.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
      ps.setString(2, paymentId);
      ps.setInt(3, accountId);
      ps.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }
  }

  /**
   * Delete Account in the system.
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion
   * @throws AccountNotFoundException if account to be deleted doesn't exist
   */
  @Override
  public boolean deleteAccount(int accountId) throws AccountNotFoundException {

    PreparedStatement ps = null;
    Connection connection = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("DELETE FROM %s WHERE %s = ?",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.ID_COLUMN);
      ps = connection.prepareStatement(sql);

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
   * GetAccount based on accountid.
   * @param accountId id of the account
   * @return null if no entry matches, Instance of {@code Account} otherwise
   */
  @Override
  public Account getAccount(int accountId) {

    Account account = null;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      connection = this.poolManager.getValidConnection();

      String sql = String.format("SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s FROM %s WHERE %s = ?",
        DBUtils.AccountTable.FIRST_NAME_COLUMN, DBUtils.AccountTable.LAST_NAME_COLUMN,
        DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
        DBUtils.AccountTable.ID_COLUMN, DBUtils.AccountTable.API_KEY_COLUMN,
        DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
        DBUtils.AccountTable.PAYMENT_INFO_PROVIDED_AT, DBUtils.AccountTable.PAYMENT_ACCOUNT_ID,
        DBUtils.AccountTable.ORG_ID,
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.ID_COLUMN);

      ps = connection.prepareStatement(sql);
      ps.setInt(1, accountId);
      rs = ps.executeQuery();

      int count = 0;
      while (rs.next()) {
        count++;
        account = new Account(rs.getString(DBUtils.AccountTable.FIRST_NAME_COLUMN),
                              rs.getString(DBUtils.AccountTable.LAST_NAME_COLUMN),
                              rs.getString(DBUtils.AccountTable.COMPANY_COLUMN),
                              rs.getString(DBUtils.AccountTable.EMAIL_COLUMN),
                              rs.getInt(DBUtils.AccountTable.ID_COLUMN),
                              rs.getString(DBUtils.AccountTable.API_KEY_COLUMN),
                              rs.getBoolean(DBUtils.AccountTable.CONFIRMED_COLUMN),
                              DBUtils.timestampToLong(rs.getTimestamp(DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT)),
                              DBUtils.timestampToLong(rs.getTimestamp(DBUtils.AccountTable.PAYMENT_INFO_PROVIDED_AT)),
                              rs.getString(DBUtils.AccountTable.PAYMENT_ACCOUNT_ID),
                              rs.getString(DBUtils.AccountTable.ORG_ID));
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
  public void updateOrganizationId(int accountId, String orgId)
    throws AccountNotFoundException, OrganizationNotFoundException {
    Connection connection = this.poolManager.getValidConnection();
    String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?",
                               DBUtils.AccountTable.TABLE_NAME,
                               DBUtils.AccountTable.ORG_ID,
                               DBUtils.AccountTable.ID_COLUMN
    );
    try {
      PreparedStatement ps = connection.prepareStatement(sql);
      try {
        ps.setString(1, orgId);
        ps.setInt(2, accountId);
        int affectedRows = ps.executeUpdate();
        connection.commit();
        if (affectedRows == 0) {
          throw new AccountNotFoundException("Account doesn't exists");
        }
      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      if (DBUtils.DB_INTEGRITY_CONSTRAINT_VIOLATION_FOR_KEY.equals(e.getSQLState())) {
        throw new OrganizationNotFoundException("Organization to be updated not found");
      } else {
        throw Throwables.propagate(e);
      }
    } finally {
      close(connection);
    }
  }

  /**
   * GetAccount based on email id.
   *
   * @param emailId emailId requested
   * @return {@code Account}
   */
  @Override
  public Account getAccount(String emailId) {

    Account account = null;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      connection = this.poolManager.getValidConnection();

      String sql = String.format("SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s FROM %s WHERE %s = ?",
        DBUtils.AccountTable.FIRST_NAME_COLUMN, DBUtils.AccountTable.LAST_NAME_COLUMN,
        DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
        DBUtils.AccountTable.ID_COLUMN, DBUtils.AccountTable.API_KEY_COLUMN,
        DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
        DBUtils.AccountTable.ORG_ID,
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.EMAIL_COLUMN);

      ps = connection.prepareStatement(sql);
      ps.setString(1, emailId);
      rs = ps.executeQuery();

      int count = 0;
      while (rs.next()) {
        count++;
        account = new Account(rs.getString(DBUtils.AccountTable.FIRST_NAME_COLUMN),
                              rs.getString(DBUtils.AccountTable.LAST_NAME_COLUMN),
                              rs.getString(DBUtils.AccountTable.COMPANY_COLUMN),
                              rs.getString(DBUtils.AccountTable.EMAIL_COLUMN),
                              rs.getInt(DBUtils.AccountTable.ID_COLUMN),
                              rs.getString(DBUtils.AccountTable.API_KEY_COLUMN),
                              rs.getBoolean(DBUtils.AccountTable.CONFIRMED_COLUMN),
                              DBUtils.timestampToLong(rs.getTimestamp(DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT)),
                              DBUtils.timestampToLong(rs.getTimestamp(DBUtils.AccountTable.PAYMENT_INFO_PROVIDED_AT)),
                              rs.getString(DBUtils.AccountTable.PAYMENT_ACCOUNT_ID),
                              rs.getString(DBUtils.AccountTable.ORG_ID));
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
  public boolean addRoleType(int accountId, Role role)  {

    Connection connection = null;
    PreparedStatement ps = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("INSERT INTO %s (%s,%s,%s,%s,%s) VALUES(?,?,?,?,?)",
        DBUtils.AccountRoleType.TABLE_NAME,
        DBUtils.AccountRoleType.ACCOUNT_ID_COLUMN,
        DBUtils.AccountRoleType.ROLE_NAME_COLUMN,
        DBUtils.AccountRoleType.PERMISSIONS_COLUMN);

      ps = connection.prepareStatement(sql);

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
  public void updateAccount(int accountId, Map<String, Object> keyValueParams) {
    Connection connection = null;
    PreparedStatement ps = null;
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

      String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ? AND %s = ?",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.PASSWORD_COLUMN,
        DBUtils.AccountTable.ID_COLUMN,
        DBUtils.AccountTable.PASSWORD_COLUMN);

      ps = connection.prepareStatement(sql);
      ps.setString(1, PasswordUtils.generateHashedPassword(newPassword));
      ps.setInt(2, accountId);
      ps.setString(3, PasswordUtils.generateHashedPassword(oldPassword));
      int count = ps.executeUpdate();
      Preconditions.checkArgument(count == 1, "Update password failed");
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
    String updateSql = String.format("UPDATE %s set %s = ? where %s = (SELECT %s from %s where %s = ?)",
      DBUtils.AccountTable.TABLE_NAME,
      DBUtils.AccountTable.PASSWORD_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
      DBUtils.Nonce.ID_COLUMN, DBUtils.Nonce.TABLE_NAME, DBUtils.Nonce.NONCE_ID_COLUMN
    );

    //Update is successful. now return the account information
    String selectSql = String.format("SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s FROM %s WHERE %s = " +
      "(SELECT %s FROM %s where %s = ?)",
      DBUtils.AccountTable.FIRST_NAME_COLUMN, DBUtils.AccountTable.LAST_NAME_COLUMN,
      DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
      DBUtils.AccountTable.ID_COLUMN, DBUtils.AccountTable.API_KEY_COLUMN,
      DBUtils.AccountTable.CONFIRMED_COLUMN,
      DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
      DBUtils.AccountTable.EMAIL_COLUMN,
      DBUtils.AccountTable.PAYMENT_INFO_PROVIDED_AT,
      DBUtils.AccountTable.PAYMENT_ACCOUNT_ID,
      DBUtils.AccountTable.ORG_ID,
      DBUtils.AccountTable.TABLE_NAME,
      DBUtils.AccountTable.EMAIL_COLUMN,
      DBUtils.Nonce.ID_COLUMN, DBUtils.Nonce.TABLE_NAME,
      DBUtils.Nonce.NONCE_ID_COLUMN
    );

    String deleteNonce  = String.format("DELETE FROM %s where %s = ?",
      DBUtils.Nonce.TABLE_NAME, DBUtils.Nonce.NONCE_ID_COLUMN);

    try {
      connection = this.poolManager.getValidConnection();
      connection.setAutoCommit(false);

      update = connection.prepareStatement(updateSql);
      update.setString(1, PasswordUtils.generateHashedPassword(newPassword));
      update.setInt(2, nonce);
      update.executeUpdate();

      select = connection.prepareStatement(selectSql);

      select.setInt(1, nonce);
      rs = select.executeQuery();

      delete = connection.prepareStatement(deleteNonce);
      delete.setInt(1, nonce);
      delete.executeUpdate();

      connection.commit();

      while (rs.next()) {
        account = new Account(rs.getString(DBUtils.AccountTable.FIRST_NAME_COLUMN),
                              rs.getString(DBUtils.AccountTable.LAST_NAME_COLUMN),
                              rs.getString(DBUtils.AccountTable.COMPANY_COLUMN),
                              rs.getString(DBUtils.AccountTable.EMAIL_COLUMN),
                              rs.getInt(DBUtils.AccountTable.ID_COLUMN),
                              rs.getString(DBUtils.AccountTable.API_KEY_COLUMN),
                              rs.getBoolean(DBUtils.AccountTable.CONFIRMED_COLUMN),
                              DBUtils.timestampToLong(rs.getTimestamp(DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT)),
                              DBUtils.timestampToLong(rs.getTimestamp(DBUtils.AccountTable.PAYMENT_INFO_PROVIDED_AT)),
                              rs.getString(DBUtils.AccountTable.PAYMENT_ACCOUNT_ID),
                              rs.getString(DBUtils.AccountTable.ORG_ID));
      }
      return account;
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(null, update);
      close(null, delete);
      close(connection, select, rs);
    }
  }

  @Override
  public void regenerateApiKey(int accountId) {
    Connection connection = null;
    PreparedStatement ps = null;

    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?",
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.AccountTable.API_KEY_COLUMN,
        DBUtils.AccountTable.ID_COLUMN);

      ps = connection.prepareStatement(sql);
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
