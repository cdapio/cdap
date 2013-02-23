/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.dal.VpcDAO;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.continuuity.passport.meta.VPC;
import com.continuuity.passport.meta.Role;

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

    String connectionString = configuration.get("connectionString");
    String jdbcType = configuration.get("jdbcType");

    if (jdbcType.toLowerCase().equals("mysql")) {
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
      connection = this.poolManager.getConnection();
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
  public void removeVPC(int accountId, int vpcId)
    throws ConfigurationException, VPCNotFoundException {

    Connection connection = null;
    PreparedStatement ps = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getConnection();

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
      connection = this.poolManager.getConnection();


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
      connection = this.poolManager.getConnection();
      String SQL = String.format("SELECT %s, %s, %s FROM %s WHERE %s = ?",
        DBUtils.VPC.VPC_ID_COLUMN, DBUtils.VPC.NAME_COLUMN,
        DBUtils.VPC.LABEL_COLUMN, //COLUMNS
        DBUtils.VPC.TABLE_NAME, //FROM
        DBUtils.VPC.ACCOUNT_ID_COLUMN); //WHERE

      ps = connection.prepareStatement(SQL);
      ps.setInt(1, accountId);
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
  public VPC getVPC(int accountId, int vpcId) throws ConfigurationException {
    VPC vpc = null;
    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    if (this.poolManager == null) {
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      connection = this.poolManager.getConnection();
      String SQL = String.format("SELECT %s, %s, %s FROM %s WHERE %s = ? and %s = ?",
        DBUtils.VPC.VPC_ID_COLUMN, DBUtils.VPC.NAME_COLUMN,
        DBUtils.VPC.LABEL_COLUMN, //COLUMNS
        DBUtils.VPC.TABLE_NAME, //FROM
        DBUtils.VPC.ACCOUNT_ID_COLUMN, //WHERE
        DBUtils.VPC.VPC_ID_COLUMN);

      ps = connection.prepareStatement(SQL);
      ps.setInt(1, accountId);
      ps.setInt(2, vpcId);
      rs = ps.executeQuery();

      while (rs.next()) {
        vpc = new VPC(rs.getInt(1), rs.getString(2), rs.getString(3));
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
      connection = this.poolManager.getConnection();
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
}
