package com.continuuity.passport.impl;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.common.sql.SQLChain;
import com.continuuity.passport.common.sql.SQLChainImpl;
import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.service.Authenticator;
import com.continuuity.passport.core.status.AuthenticationStatus;
import com.continuuity.passport.dal.db.Common;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */

public class AuthenticatorImpl extends AuthorizingRealm implements Authenticator {

  private Map<String,String> configurations;
  //TODO: move this to a singleton class
  private DBConnectionPoolManager poolManager =null;


  /**
   * Authenticates User with the Credentials passed
   *
   * @param userId      User to be authenticated
   * @param credentials UserCredentials that authenticates the user
   * @return {@code AuthenticationStatus}
   * @throws {@code RetryException}
   */
  @Override
  public AuthenticationStatus authenticate(String userId, Credentials credentials) throws RetryException {
    return null;
  }

  @Override
  public void configure(Map<String, String> configurations) {
    this.configurations = configurations;

    String connectionString = this.configurations.get("connectionString");
    String jdbcType = this.configurations.get("jdbcType");

    if (jdbcType.toLowerCase().equals("mysql")) {

      MysqlConnectionPoolDataSource mysqlDataSource =  new MysqlConnectionPoolDataSource();
      mysqlDataSource.setUrl(connectionString);
      this.poolManager = new DBConnectionPoolManager(mysqlDataSource, 20);

    }
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
    if ( principals == null) {
     throw new AuthorizationException("PrincipalCollection argument cannot be null");
    }

    String username = (String) getAvailablePrincipal(principals);
    Set<String> rolePermissions = null;
    Set<String> roleNames = null;
    SimpleAuthorizationInfo info = null;
    try {
      Connection connection = this.poolManager.getConnection();
      SQLChain chain = SQLChainImpl.getSqlChain(connection);
      List<Map<String,Object>> resultSet = chain.selectWithJoin(Common.VPCRole.TABLE_NAME,Common.AccountRole.TABLE_NAME)
                                                .joinOn().condition(String.format("%s.%s = %s.%s",
                                                                                  Common.VPC.TABLE_NAME,
                                                                                  Common.VPC.VPC_ID_COLUMN,
                                                                                  Common.VPCRole.TABLE_NAME,
                                                                                  Common.VPCRole.VPC_ID_COLUMN))
                                                .where(String.format("%s.%s",Common.VPCRole.USER_ID_COLUMN))
                                                            .equal(username)
                                                .execute();


      String roleNameKey = String.format("%s.%s",Common.VPC.TABLE_NAME.toUpperCase(),
                                              Common.VPC.NAME_COLUMN.toUpperCase());

      String permissionsKey = String.format("%s.%s",Common.AccountRole.TABLE_NAME,
                                                    Common.AccountRole.PERMISSIONS_COLUMN);

      String permissionOverridesKey = String.format("%s.%s",Common.VPCRole.TABLE_NAME,
                                                            Common.VPCRole.ROLE_OVERRIDES_COLUMN);

      for(Map<String,Object> result : resultSet) {
        roleNames.add((String) result.get(roleNameKey));
        String overrides  = (String) result.get(permissionOverridesKey);
        if (overrides == null) {
          rolePermissions.add((String)result.get(permissionsKey));
        }
        else {
          rolePermissions.add(overrides);
        }
      }
      info  = new SimpleAuthorizationInfo(roleNames);
      info.setStringPermissions(rolePermissions);

    } catch (SQLException e) {
      //TODO: Log and throw exception
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }


    return info;
  }

  /**
   * Retrieves authentication data from an implementation-specific datasource (RDBMS, LDAP, etc) for the given
   * authentication token.
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
  protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {


    UsernamePasswordToken upToken = (UsernamePasswordToken) token;
    String username = upToken.getUsername();

    SimpleAuthenticationInfo info = null;
    try {
      Connection connection = this.poolManager.getConnection();
      SQLChain chain  = SQLChainImpl.getSqlChain(connection);
      List<Map<String,Object>> resultSet = chain.select(Common.AccountTable.TABLE_NAME)
                                                .include(Common.AccountTable.PASSWORD_COLUMN)
                                                .where(Common.AccountTable.EMAIL_COLUMN).equal(username)
                                                .execute();

      if (resultSet.size() == 1 ) {
        String password = (String) resultSet.get(0).get(Common.AccountTable.PASSWORD_COLUMN.toUpperCase());
        info = new SimpleAuthenticationInfo(username,password,getName());
      }

    } catch (SQLException e) {
      //TODO: Log and throw exception
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return info;

  }
}
