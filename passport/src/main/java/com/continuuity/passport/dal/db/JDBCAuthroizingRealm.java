/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.security.UsernamePasswordApiKeyToken;
import com.continuuity.passport.meta.Account;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * Shiro Realm that is responsible for Communicating with Database to get the credentials for authentication
 * and authorizations.
 */
public class JDBCAuthroizingRealm extends AuthorizingRealm {
  private final DBConnectionPoolManager poolManager;

  private static final String SQL_LOOKUP_BY_EMAIL = String.format("SELECT %s, %s, %s, %s, %s, %s," +
    "%s, %s FROM %s WHERE %s = ?",
    DBUtils.AccountTable.FIRST_NAME_COLUMN,
    DBUtils.AccountTable.LAST_NAME_COLUMN,
    DBUtils.AccountTable.COMPANY_COLUMN,
    DBUtils.AccountTable.ID_COLUMN,
    DBUtils.AccountTable.PASSWORD_COLUMN,
    DBUtils.AccountTable.API_KEY_COLUMN,
    DBUtils.AccountTable.CONFIRMED_COLUMN,
    DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
    DBUtils.AccountTable.TABLE_NAME,
    DBUtils.AccountTable.EMAIL_COLUMN);

  private static final String SQL_LOOKUP_BY_APIKEY = String.format("SELECT %s, %s, %s, %s, %s, %s," +
    "%s, %s FROM %s WHERE %s = ?",
    DBUtils.AccountTable.FIRST_NAME_COLUMN,
    DBUtils.AccountTable.LAST_NAME_COLUMN,
    DBUtils.AccountTable.COMPANY_COLUMN,
    DBUtils.AccountTable.ID_COLUMN,
    DBUtils.AccountTable.PASSWORD_COLUMN,
    DBUtils.AccountTable.API_KEY_COLUMN,
    DBUtils.AccountTable.CONFIRMED_COLUMN,
    DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
    DBUtils.AccountTable.TABLE_NAME,
    DBUtils.AccountTable.API_KEY_COLUMN);


  @Inject
  public JDBCAuthroizingRealm(DBConnectionPoolManager poolManager) {
    Preconditions.checkNotNull(poolManager, "PoolManager  should not be null");
    this.poolManager = poolManager;
  }

  @Override
  protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {
    Preconditions.checkNotNull(principals);

    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    int accountId = (Integer) getAvailablePrincipal(principals);
    Set<String> rolePermissions = null;
    Set<String> roleNames = null;
    SimpleAuthorizationInfo info = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("SELECT %s,%s,%s FROM %s JOIN %s ON %s = %s WHERE %s = ?",
        //SELECT COLS
        DBUtils.AccountRoleType.TABLE_NAME + "." + DBUtils.AccountRoleType.ROLE_NAME_COLUMN,
        DBUtils.AccountRoleType.TABLE_NAME + "." + DBUtils.AccountRoleType.PERMISSIONS_COLUMN,
        DBUtils.VPCRole.TABLE_NAME + "." + DBUtils.VPCRole.ROLE_OVERRIDES_COLUMN,

        //TABLE NAMES
        DBUtils.AccountRoleType.TABLE_NAME, DBUtils.VPCRole.TABLE_NAME,

        //JOIN CONDITION
        DBUtils.AccountRoleType.TABLE_NAME + "." + DBUtils.AccountRoleType.ACCOUNT_ID_COLUMN,
        DBUtils.VPCRole.TABLE_NAME + "." + DBUtils.VPCRole.ACCOUNT_ID_COLUMN,

        //WHERE CLAUSE
        DBUtils.VPCRole.USER_ID_COLUMN);

      ps = connection.prepareStatement(sql);
      ps.setInt(1, accountId);
      rs = ps.executeQuery();

      while (rs.next()) {
        String roleName = rs.getString(1);
        String permissions = rs.getString(2);
        String overrides = rs.getString(3);
        if (overrides != null && !overrides.isEmpty()) {
          rolePermissions.add(overrides);
        } else {
          rolePermissions.add(permissions);
        }
        roleNames.add(roleName);
      }

      info = new SimpleAuthorizationInfo(roleNames);
      info.setStringPermissions(rolePermissions);
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
        if (ps != null) {
          ps.close();
        }
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException e) {
        throw Throwables.propagate(e);
      }
    }
    return info;
  }

  @Override
  protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) {
    UsernamePasswordApiKeyToken upToken = (UsernamePasswordApiKeyToken) token;
    String emailId = upToken.getUsername();
    String apiKey = upToken.getApiKey();
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    SimpleAuthenticationInfo info = null;
    try {
      connection = this.poolManager.getValidConnection();

      String sql = null;
      ps = null;

      if (upToken.isUseApiKey()) {
        sql = SQL_LOOKUP_BY_APIKEY;
        ps = connection.prepareStatement(sql);
        ps.setString(1, apiKey);
      } else {
        sql = SQL_LOOKUP_BY_EMAIL;
        ps = connection.prepareStatement(sql);
        ps.setString(1, emailId);
      }

      Preconditions.checkNotNull(ps, "ApiKey or emailId should be set.");

      rs = ps.executeQuery();

      int count = 0;
      String password = null;
      int accountId = -1;
      String firstName = null;
      String lastName = null;
      String company = null;
      String apiToken = null;
      long devsuiteDownloadedTime = -1;
      boolean confirmed = false;
      while (rs.next()) {
        firstName = rs.getString(1);
        lastName = rs.getString(2);
        company = rs.getString(3);
        accountId = rs.getInt(4);
        password = rs.getString(5);

        apiToken = rs.getString(6);
        confirmed = rs.getBoolean(7);
        devsuiteDownloadedTime = DBUtils.timestampToLong(rs.getTimestamp(8));
        count++;
      }

      Preconditions.checkArgument(count == 1, "Account not found in DB");
      Preconditions.checkArgument(!password.isEmpty(), "Password not found for %s in the data store", emailId);
      Account account = new Account(firstName, lastName, company, emailId, accountId,
        apiToken, confirmed, devsuiteDownloadedTime);

      //if we are authenticating with API Key then existence of apiKey with a password is authenticating.
      // So set the password to a dummy password
      if (upToken.isUseApiKey()) {
        info = new SimpleAuthenticationInfo(account, UsernamePasswordApiKeyToken.DUMMY_PASSWORD, getName());
      } else {
        info = new SimpleAuthenticationInfo(account, password, getName());
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
        if (ps != null) {
          ps.close();
        }
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException e) {
        throw Throwables.propagate(e);
      }
    }
    return info;
  }
}
