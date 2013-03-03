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

import javax.sql.ConnectionPoolDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * Shiro Realm that is responsible for Communicating with Database to get the credentials for authentication
 * and authorizations
 */
public class JDBCAuthroizingRealm extends AuthorizingRealm {
  private DBConnectionPoolManager poolManager = null;

  private final String SQL_LOOKUP_BY_EMAIL = String.format("SELECT %s, %s, %s, %s, %s, %s," +
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

  private final String SQL_LOOKUP_BY_APIKEY = String.format("SELECT %s, %s, %s, %s, %s, %s," +
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
  public JDBCAuthroizingRealm(ConnectionPoolDataSource dataSource) {
    Preconditions.checkNotNull(dataSource, "Data source should not be null");
    this.poolManager = new DBConnectionPoolManager(dataSource, 20);
  }

  /**
   * Retrieves the AuthorizationInfo for the given principals from the underlying data store.  When returning
   * an instance from this method, you might want to consider using an instance of
   * {@link org.apache.shiro.authz.SimpleAuthorizationInfo SimpleAuthorizationInfo}, as it is suitable in most cases.
   *
   * @param principals the primary identifying principals of the AuthorizationInfo that should be retrieved.
   * @return the AuthorizationInfo associated with this principals.
   * @see org.apache.shiro.authz.SimpleAuthorizationInfo
   */
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
      String SQL = String.format("SELECT %s,%s,%s FROM %s JOIN %s ON %s = %s WHERE %s = ?",
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

      ps = connection.prepareStatement(SQL);
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

  /**
   * Retrieves authentication data from RDBMS for the given authentication token.
   * <p/>
   * For most datasources, this means just 'pulling' authentication data for an associated subject/user and nothing
   * more and letting Shiro do the rest.  But in some systems, this method could actually perform EIS specific
   * log-in logic in addition to just retrieving data - it is up to the Realm implementation.
   * <p/>
   * A {@code null} return value means that no account could be associated with the specified token.
   *
   * @param token the authentication token containing the user's principal and credentials.
   * @return an {@link org.apache.shiro.authc.AuthenticationInfo} object containing account data resulting from the
   *         authentication ONLY if the lookup is successful (i.e. account exists and is valid, etc.)
   * @throws org.apache.shiro.authc.AuthenticationException
   *          if there is an error acquiring data or performing
   *          realm-specific authentication logic for the specified <tt>token</tt>
   */
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

      String SQL = null;
      ps = null;

      if (upToken.isUseApiKey()) {
        SQL = SQL_LOOKUP_BY_APIKEY;
        ps = connection.prepareStatement(SQL);
        ps.setString(1, apiKey);
      } else {
        SQL = SQL_LOOKUP_BY_EMAIL;
        ps = connection.prepareStatement(SQL);
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
        devsuiteDownloadedTime = DBUtils.getDevsuiteDownloadedTime(rs.getTimestamp(8));
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
