/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.Constants;
import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Role;
import com.continuuity.passport.meta.VPC;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class VpcDBAccess extends DBAccess implements VpcDAO {

  private Map<String, String> configuration;

  private DBConnectionPoolManager poolManager = null;


  @Inject
  public void VpcDBAccess(@Named("passport.config") Map<String, String> configuration) {

    String connectionString = configuration.get(Constants.CFG_JDBC_CONNECTION_STRING);
    String jdbcType = configuration.get(Constants.CFG_JDBC_TYPE);

    if (jdbcType.toLowerCase().equals(Constants.DEFAULT_JDBC_TYPE)) {
      MysqlConnectionPoolDataSource mysqlDataSource = new MysqlConnectionPoolDataSource();
      mysqlDataSource.setUrl(connectionString);
      this.poolManager = new DBConnectionPoolManager(mysqlDataSource, 20);
    }
  }

  @Override
  public VPC addVPC(int accountId, VPC vpc) throws ConfigurationException {
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet result = null;
    Preconditions.checkNotNull(this.poolManager, "DBConnection pool is null. DAO is not configured");

    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES (?,?,?,?)",
        DBUtils.VPC.TABLE_NAME,
        DBUtils.VPC.ACCOUNT_ID_COLUMN, DBUtils.VPC.NAME_COLUMN,
        DBUtils.VPC.LABEL_COLUMN,
        DBUtils.VPC.VPC_CREATED_AT);

      Date date = new Date();
      ps = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
      ps.setInt(1, accountId);
      ps.setString(2, vpc.getVpcName());
      ps.setString(3, vpc.getVpcLabel());
      ps.setTimestamp(4, new java.sql.Timestamp(date.getTime()));

      ps.executeUpdate();

      result = ps.getGeneratedKeys();
      if (result == null) {
        throw new RuntimeException("Failed Insert");
      }
      result.next();
      return new VPC(result.getInt(1), vpc.getVpcName(), vpc.getVpcLabel());
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps, result);
    }
  }

  @Override
  public void removeVPC(String vpcName) throws VPCNotFoundException{
    Preconditions.checkNotNull(this.poolManager,"Data source connector cannot be null");
    Connection connection = null;
    PreparedStatement ps = null;
    String SQL = String.format("DELETE FROM %s where %s = ?",DBUtils.VPC.TABLE_NAME,DBUtils.VPC.NAME_COLUMN);
    try {
      connection =  this.poolManager.getValidConnection();
      ps = connection.prepareStatement(SQL);
      ps.setString(1,vpcName);
      int count = ps.executeUpdate();
      if (count == 0 ) {
        throw new VPCNotFoundException("VPC not found");
      }
    } catch (SQLException e){
      throw Throwables.propagate(e);
    } finally {
      close(connection,ps);
    }
  }

  @Override
  public void removeVPC(int accountId, int vpcId)
    throws ConfigurationException, VPCNotFoundException {

    Connection connection = null;
    PreparedStatement ps = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();

      String SQL = String.format("DELETE FROM %s WHERE %s = ? and %s = ?",
        DBUtils.VPC.TABLE_NAME,
        DBUtils.VPC.VPC_ID_COLUMN,
        DBUtils.VPC.ACCOUNT_ID_COLUMN);

      ps = connection.prepareStatement(SQL);
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
  public boolean addRoles(int accountId, int vpcId, int userId, Role role, String overrides)
    throws ConfigurationException {

    Connection connection = null;
    PreparedStatement ps = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();


      String SQL = String.format("INSERT INTO %s (%s,%s,%s,%s,%s) VALUES (?,?,?,?,?)",
        DBUtils.VPCRole.TABLE_NAME,
        DBUtils.VPCRole.VPC_ID_COLUMN, DBUtils.VPCRole.ACCOUNT_ID_COLUMN,
        DBUtils.VPCRole.USER_ID_COLUMN, DBUtils.VPCRole.ROLE_TYPE_COLUMN,
        DBUtils.VPCRole.ROLE_OVERRIDES_COLUMN);

      ps = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
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
  public List<VPC> getVPC(int accountId) throws ConfigurationException {

    List<VPC> vpcList = new ArrayList<VPC>();
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("SELECT %s, %s, %s, %s FROM %s WHERE %s = ?",
        DBUtils.VPC.VPC_ID_COLUMN, DBUtils.VPC.NAME_COLUMN,
        DBUtils.VPC.LABEL_COLUMN,
        DBUtils.VPC.VPC_CREATED_AT,//COLUMNS
        DBUtils.VPC.TABLE_NAME, //FROM
        DBUtils.VPC.ACCOUNT_ID_COLUMN); //WHERE

      ps = connection.prepareStatement(SQL);
      ps.setInt(1, accountId);
      rs = ps.executeQuery();

      while (rs.next()) {
        VPC vpc = new VPC(rs.getInt(1), rs.getString(2), rs.getString(3),DBUtils.timestampToLong(rs.getTimestamp(4)));
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
  public VPC getVPC(int accountId, int vpcId) throws ConfigurationException {
    VPC vpc = null;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("SELECT %s, %s, %s, %s FROM %s WHERE %s = ? and %s = ?",
        DBUtils.VPC.VPC_ID_COLUMN, DBUtils.VPC.NAME_COLUMN,
        DBUtils.VPC.LABEL_COLUMN,
        DBUtils.VPC.VPC_CREATED_AT,//COLUMNS
        DBUtils.VPC.TABLE_NAME, //FROM
        DBUtils.VPC.ACCOUNT_ID_COLUMN, //WHERE
        DBUtils.VPC.VPC_ID_COLUMN);

      ps = connection.prepareStatement(SQL);
      ps.setInt(1, accountId);
      ps.setInt(2, vpcId);
      rs = ps.executeQuery();

      while (rs.next()) {
        vpc = new VPC(rs.getInt(1), rs.getString(2), rs.getString(3),DBUtils.timestampToLong(rs.getTimestamp(4)));
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps, rs);
    }
    return vpc;
  }

  @Override
  public List<VPC> getVPC(String apiKey) throws ConfigurationException {
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    List<VPC> vpcList = new ArrayList<VPC>();
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("SELECT %s, %s, %s FROM %s JOIN %s ON %s = %s WHERE %s = ?",
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.VPC_ID_COLUMN,
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.NAME_COLUMN, //COLUMNS
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.LABEL_COLUMN, //COLUMNS
        DBUtils.VPC.TABLE_NAME, //FROM
        DBUtils.AccountTable.TABLE_NAME, //JOIN
        DBUtils.VPC.TABLE_NAME + "." + DBUtils.VPC.ACCOUNT_ID_COLUMN, //CONDITION
        DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.ID_COLUMN,
        DBUtils.AccountTable.TABLE_NAME + "." + DBUtils.AccountTable.API_KEY_COLUMN);

      ps = connection.prepareStatement(SQL);
      ps.setString(1, apiKey);
      rs = ps.executeQuery();

      while (rs.next()) {
        VPC vpc = new VPC(rs.getInt(1), rs.getString(2), rs.getString(3));
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
  public int getVPCCount(String vpcName) {
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    int count = 0;
    Preconditions.checkNotNull(this.poolManager, "DBConnection pool is null. DAO is not configured");

    try {
      connection = this.poolManager.getValidConnection();
      String SQL = String.format("SELECT COUNT(%s) FROM %s where %s = ? ",
        DBUtils.VPC.NAME_COLUMN, DBUtils.VPC.TABLE_NAME, DBUtils.VPC.NAME_COLUMN);

      ps = connection.prepareStatement(SQL);
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
  public Account getAccountForVPC(String vpcName) {
    Account account = null;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    Preconditions.checkNotNull(this.poolManager,"Connection pool is null");

    try {
      connection = this.poolManager.getValidConnection();

      String SQL = String.format("SELECT %s,%s,%s,%s,%s,%s,%s,%s FROM %s JOIN %s ON %s = %s WHERE %s = ?",
        DBUtils.AccountTable.FIRST_NAME_COLUMN, DBUtils.AccountTable.LAST_NAME_COLUMN,
        DBUtils.AccountTable.COMPANY_COLUMN, DBUtils.AccountTable.EMAIL_COLUMN,
        DBUtils.AccountTable.TABLE_NAME+"."+ DBUtils.AccountTable.ID_COLUMN,   // Dis-ambiguate id column
        DBUtils.AccountTable.API_KEY_COLUMN,
        DBUtils.AccountTable.CONFIRMED_COLUMN, DBUtils.AccountTable.DEV_SUITE_DOWNLOADED_AT, //SELECT
        DBUtils.AccountTable.TABLE_NAME,
        DBUtils.VPC.TABLE_NAME,   // JOIN
        DBUtils.AccountTable.TABLE_NAME + "."+DBUtils.AccountTable.ID_COLUMN,
        DBUtils.VPC.TABLE_NAME + "."+DBUtils.VPC.ACCOUNT_ID_COLUMN, //JOIN Condition
        DBUtils.VPC.NAME_COLUMN // WHERE clause
      );

      ps = connection.prepareStatement(SQL);
      ps.setString(1, vpcName);
      rs = ps.executeQuery();

      int count = 0;
      while (rs.next()) {
        count++;
        account = new Account(rs.getString(1), rs.getString(2), rs.getString(3),
          rs.getString(4), rs.getInt(5), rs.getString(6),
          rs.getBoolean(7), DBUtils.timestampToLong(rs.getTimestamp(8)));
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
