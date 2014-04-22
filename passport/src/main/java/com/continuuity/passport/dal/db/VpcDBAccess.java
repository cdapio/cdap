/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Role;
import com.continuuity.passport.meta.RolesAccounts;
import com.continuuity.passport.meta.VPC;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 */
public class VpcDBAccess extends DBAccess implements VpcDAO {

  private final DBConnectionPoolManager poolManager;
  private static final Logger LOG = LoggerFactory.getLogger(VpcDBAccess.class);
  private static final String DEFAULT_ROLE = "admin"; //TODO: (ENG-2205) - resolved by using authorization using shiro
  @Inject
  public VpcDBAccess(DBConnectionPoolManager poolManager) {
    this.poolManager = poolManager;
  }

  @Override
  public VPC addVPC(int accountId, VPC vpc) {
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet result = null;

    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?)",
        DBUtils.VPC.TABLE_NAME,
        DBUtils.VPC.ACCOUNT_ID_COLUMN, DBUtils.VPC.NAME_COLUMN,
        DBUtils.VPC.LABEL_COLUMN,
        DBUtils.VPC.VPC_CREATED_AT,
        DBUtils.VPC.VPC_TYPE);

      Date date = new Date();
      ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
      ps.setInt(1, accountId);
      ps.setString(2, vpc.getVpcName());
      ps.setString(3, vpc.getVpcLabel());
      ps.setTimestamp(4, new java.sql.Timestamp(date.getTime()));
      ps.setString(5, vpc.getVpcType());

      ps.executeUpdate();
      //TODO: (ENG-2215)
      //configuration This is not an issue while working in non-test mode (mysql)
      connection.commit();
      result = ps.getGeneratedKeys();
      if (result == null) {
        throw new RuntimeException("Failed Insert");
      }
      result.next();
      return new VPC(result.getInt(1), vpc.getVpcName(), vpc.getVpcLabel(), vpc.getVpcType());
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps, result);
    }
  }

  @Override
  public void removeVPC(String vpcName) throws VPCNotFoundException {
    Connection connection = null;
    PreparedStatement ps = null;
    String sql = String.format("DELETE FROM %s where %s = ?", DBUtils.VPC.TABLE_NAME, DBUtils.VPC.NAME_COLUMN);
    try {
      connection =  this.poolManager.getValidConnection();
      ps = connection.prepareStatement(sql);
      ps.setString(1, vpcName);
      int count = ps.executeUpdate();
      if (count == 0) {
        throw new VPCNotFoundException("VPC not found");
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }
  }

  @Override
  public void removeVPC(int accountId, int vpcId) throws VPCNotFoundException {

    Connection connection = null;
    PreparedStatement ps = null;
    try {
      connection = this.poolManager.getValidConnection();

      String sql = String.format("DELETE FROM %s WHERE %s = ? and %s = ?",
        DBUtils.VPC.TABLE_NAME,
        DBUtils.VPC.VPC_ID_COLUMN,
        DBUtils.VPC.ACCOUNT_ID_COLUMN);

      ps = connection.prepareStatement(sql);
      ps.setInt(1, vpcId);
      ps.setInt(2, accountId);

      int count = ps.executeUpdate();
      if (count == 0) {
        throw new VPCNotFoundException("VPC not found");
      }

    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }
  }

  @Override
  public boolean addRoles(int accountId, int vpcId, int userId, Role role, String overrides) {

    Connection connection = null;
    PreparedStatement ps = null;
    try {
      connection = this.poolManager.getValidConnection();


      String sql = String.format("INSERT INTO %s (%s,%s,%s,%s,%s) VALUES (?, ?, ?, ?, ?)",
        DBUtils.VPCRole.TABLE_NAME,
        DBUtils.VPCRole.VPC_ID_COLUMN, DBUtils.VPCRole.ACCOUNT_ID_COLUMN,
        DBUtils.VPCRole.USER_ID_COLUMN, DBUtils.VPCRole.ROLE_TYPE_COLUMN,
        DBUtils.VPCRole.ROLE_OVERRIDES_COLUMN);

      ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
      ps.setInt(1, vpcId);
      ps.setInt(2, accountId);
      ps.setInt(3, userId);
      ps.setString(4, role.getRoleType());
      ps.setString(5, overrides);
      ps.executeUpdate();

    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }


  @Override
  public List<VPC> getVPC(int accountId)  {

    List<VPC> vpcList = new ArrayList<VPC>();
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s = ? OR %s IN (" +
                                   "SELECT %s from %s WHERE %s = ?)",
        DBUtils.VPC.VPC_ID_COLUMN, DBUtils.VPC.NAME_COLUMN,
        DBUtils.VPC.LABEL_COLUMN,
        DBUtils.VPC.VPC_CREATED_AT,
        DBUtils.VPC.VPC_TYPE, //COLUMNS
        DBUtils.VPC.TABLE_NAME, //FROM
        DBUtils.VPC.ACCOUNT_ID_COLUMN,
        DBUtils.VPC.VPC_ID_COLUMN,
        DBUtils.VPCRole.VPC_ID_COLUMN,
        DBUtils.VPCRole.TABLE_NAME,
        DBUtils.VPCRole.USER_ID_COLUMN
        );

      ps = connection.prepareStatement(sql);
      ps.setInt(1, accountId);
      ps.setInt(2, accountId);
      rs = ps.executeQuery();

      while (rs.next()) {
        VPC vpc = new VPC(rs.getInt(DBUtils.VPC.VPC_ID_COLUMN), rs.getString(DBUtils.VPC.NAME_COLUMN),
                          rs.getString(DBUtils.VPC.LABEL_COLUMN),
                          DBUtils.timestampToLong(rs.getTimestamp(DBUtils.VPC.VPC_CREATED_AT)),
                          rs.getString(DBUtils.VPC.VPC_TYPE));
        vpcList.add(vpc);
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps, rs);
    }
    return vpcList;
  }

  @Override
  public VPC getVPC(int accountId, int vpcId) {
    VPC vpc = null;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s = ? and %s = ?",
        DBUtils.VPC.VPC_ID_COLUMN, DBUtils.VPC.NAME_COLUMN,
        DBUtils.VPC.LABEL_COLUMN,
        DBUtils.VPC.VPC_CREATED_AT,
        DBUtils.VPC.VPC_TYPE, //COLUMNS
        DBUtils.VPC.TABLE_NAME, //FROM
        DBUtils.VPC.ACCOUNT_ID_COLUMN, //WHERE
        DBUtils.VPC.VPC_ID_COLUMN);

      ps = connection.prepareStatement(sql);
      ps.setInt(1, accountId);
      ps.setInt(2, vpcId);
      rs = ps.executeQuery();

      while (rs.next()) {
        vpc = new VPC(rs.getInt(DBUtils.VPC.VPC_ID_COLUMN), rs.getString(DBUtils.VPC.NAME_COLUMN),
                          rs.getString(DBUtils.VPC.LABEL_COLUMN),
                          DBUtils.timestampToLong(rs.getTimestamp(DBUtils.VPC.VPC_CREATED_AT)),
                          rs.getString(DBUtils.VPC.VPC_TYPE));
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps, rs);
    }
    return vpc;
  }

  @Override
  public List<VPC> getVPC(String apiKey) {
    //The VPCs that are returned is a combination of vpcs that is owned by the user with the apiKey and
    //the VPCs where the user with the apiKey has access to.
    //This is done using two queries for now.
    Connection connection = null;
    PreparedStatement ps = null;
    PreparedStatement psRoleBased = null;
    ResultSet rs = null;
    ResultSet rsRoleBased = null;
    List<VPC> vpcList = new ArrayList<VPC>();
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("SELECT %s, %s, %s, %s, %s FROM %s JOIN %s ON %s = %s WHERE %s = ?",
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.VPC_ID_COLUMN,
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.NAME_COLUMN,
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.LABEL_COLUMN,
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.VPC_CREATED_AT,
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.VPC_TYPE,   //COLUMNS
        DBUtils.VPC.TABLE_NAME, //FROM
        DBUtils.AccountTable.TABLE_NAME, //JOIN
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.ACCOUNT_ID_COLUMN, //CONDITION
        DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.ID_COLUMN,
        DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.API_KEY_COLUMN);

      ps = connection.prepareStatement(sql);
      ps.setString(1, apiKey);
      rs = ps.executeQuery();

      while (rs.next()) {
        VPC vpc = new VPC(rs.getInt(DBUtils.VPC.VPC_ID_COLUMN), rs.getString(DBUtils.VPC.NAME_COLUMN),
                          rs.getString(DBUtils.VPC.LABEL_COLUMN),
                          DBUtils.timestampToLong(rs.getTimestamp(DBUtils.VPC.VPC_CREATED_AT)),
                          rs.getString(DBUtils.VPC.VPC_TYPE));
        vpcList.add(vpc);
      }

      String sqlRoleBased =  String.format("SELECT %s, %s, %s, %s, %s FROM %s JOIN %s ON %s = %s WHERE %s IN (" +
                                             "SELECT %s from %s WHERE %s = ? ) ",
                                           DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.VPC_ID_COLUMN,
                                           DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.NAME_COLUMN,
                                           DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.LABEL_COLUMN,
                                           DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.VPC_CREATED_AT,
                                           DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.VPC_TYPE,   //COLUMNS
                                           DBUtils.VPC.TABLE_NAME, //FROM
                                           DBUtils.VPCRole.TABLE_NAME,
                                           DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.VPC_ID_COLUMN, //CONDITION
                                           DBUtils.VPCRole.TABLE_NAME + "." + DBUtils.VPCRole.VPC_ID_COLUMN, //CONDITION
                                           DBUtils.VPCRole.TABLE_NAME + "." + DBUtils.VPCRole.ACCOUNT_ID_COLUMN, //WHERE
                                           DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.ID_COLUMN,
                                           DBUtils.AccountTable.TABLE_NAME,
                                           DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.API_KEY_COLUMN);

      psRoleBased = connection.prepareStatement(sqlRoleBased);
      psRoleBased.setString(1, apiKey);
      rsRoleBased = psRoleBased.executeQuery();

      while (rsRoleBased.next()) {
        VPC vpc = new VPC(rs.getInt(DBUtils.VPC.VPC_ID_COLUMN), rs.getString(DBUtils.VPC.NAME_COLUMN),
                          rs.getString(DBUtils.VPC.LABEL_COLUMN),
                          DBUtils.timestampToLong(rs.getTimestamp(DBUtils.VPC.VPC_CREATED_AT)),
                          rs.getString(DBUtils.VPC.VPC_TYPE));
        vpcList.add(vpc);
      }

    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(null, psRoleBased, rsRoleBased);
      close(connection, ps, rs);
    }
    return vpcList;
  }

  @Override
  public int getVPCCount(String vpcName) {
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    int count = 0;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("SELECT COUNT(%s) FROM %s where %s = ? ",
        DBUtils.VPC.NAME_COLUMN, DBUtils.VPC.TABLE_NAME, DBUtils.VPC.NAME_COLUMN);

      ps = connection.prepareStatement(sql);
      ps.setString(1, vpcName);
      rs = ps.executeQuery();

      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps, rs);
    }
    return count;
  }

  @Override
  public RolesAccounts getRolesAccounts(String vpcName) {

    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    RolesAccounts rolesAccounts = new RolesAccounts();
    try {
     connection = this.poolManager.getValidConnection();

      String sql = String.format("SELECT %s, %s, %s, %s, %s, %s, %s, %s FROM %s " +
                                 "JOIN %s ON %s = %s " +
                                 "JOIN %s ON %s = %s " +
                                 "WHERE %s = ?  ",
                                 DBUtils.AccountTable.FIRST_NAME_COLUMN, DBUtils.AccountTable.LAST_NAME_COLUMN, //SELECT
                                 DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
                                 DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.ID_COLUMN,
                                 DBUtils.AccountTable.API_KEY_COLUMN,
                                 DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT,
                                 DBUtils.AccountTable.TABLE_NAME,
                                 DBUtils.VPCRole.TABLE_NAME, //JOIN
                                 DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.ID_COLUMN,
                                 DBUtils.VPCRole.TABLE_NAME + "." + DBUtils.VPCRole.ACCOUNT_ID_COLUMN,
                                 DBUtils.VPC.TABLE_NAME, //JOIN
                                 DBUtils.VPCRole.TABLE_NAME + "." + DBUtils.VPCRole.VPC_ID_COLUMN,
                                 DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.VPC_ID_COLUMN,
                                 DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.NAME_COLUMN
                                 );
      ps = connection.prepareStatement(sql);
      ps.setString(1, vpcName);
      rs = ps.executeQuery();

      while (rs.next()) {
        Account account = new Account(rs.getString(DBUtils.AccountTable.FIRST_NAME_COLUMN),
                                      rs.getString(DBUtils.AccountTable.LAST_NAME_COLUMN),
                                      rs.getString(DBUtils.AccountTable.COMPANY_COLUMN),
                                      rs.getString(DBUtils.AccountTable.EMAIL_COLUMN),
                                      rs.getInt(DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.ID_COLUMN),
                                      rs.getString(DBUtils.AccountTable.API_KEY_COLUMN),
                                      rs.getBoolean(DBUtils.AccountTable.CONFIRMED_COLUMN),
                                      DBUtils.timestampToLong(rs.getTimestamp(
                                                                DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT)));
        rolesAccounts.addAccountRole(DEFAULT_ROLE, account);
      }

      return rolesAccounts;

    } catch (SQLException e) {
      LOG.error(String.format("Caught exception while running DB query for getAccountRole. Error %s", e.getMessage()));
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps, rs);
    }
  }

  @Override
  public Account getAccountForVPC(String vpcName) {
    Account account = null;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      connection = this.poolManager.getValidConnection();

      String sql = String.format("SELECT %s,%s,%s,%s,%s,%s,%s,%s FROM %s JOIN %s ON %s = %s WHERE %s = ?",
        DBUtils.AccountTable.FIRST_NAME_COLUMN, DBUtils.AccountTable.LAST_NAME_COLUMN,
        DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
        DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.ID_COLUMN,   // Dis-ambiguate id column
        DBUtils.AccountTable.API_KEY_COLUMN,
        DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT, //SELECT
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.VPC.TABLE_NAME,   // JOIN
        DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.ID_COLUMN,
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.ACCOUNT_ID_COLUMN, //JOIN Condition
        DBUtils.VPC.NAME_COLUMN // WHERE clause
      );

      ps = connection.prepareStatement(sql);
      ps.setString(1, vpcName);
      rs = ps.executeQuery();

      int count = 0;
      while (rs.next()) {
        count++;
        account = new Account(rs.getString(DBUtils.AccountTable.FIRST_NAME_COLUMN),
                              rs.getString(DBUtils.AccountTable.LAST_NAME_COLUMN),
                              rs.getString(DBUtils.AccountTable.COMPANY_COLUMN),
                              rs.getString(DBUtils.AccountTable.EMAIL_COLUMN),
                              rs.getInt(DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.ID_COLUMN),
                              rs.getString(DBUtils.AccountTable.API_KEY_COLUMN),
                              rs.getBoolean(DBUtils.AccountTable.CONFIRMED_COLUMN),
                              DBUtils.timestampToLong(rs.getTimestamp(DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT)));
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

}
