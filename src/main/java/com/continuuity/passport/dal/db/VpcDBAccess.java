package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.meta.Role;
import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.dal.VpcDAO;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class VpcDBAccess implements VpcDAO {

  private Map<String, String> configuration;

  private DBConnectionPoolManager poolManager =null;

  @Override
  public VPC addVPC(int accountId, VPC vpc) throws ConfigurationException, RuntimeException {
    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection= this.poolManager.getConnection();

      String SQL = String.format( "INSERT INTO %s (%s, %s, %s) VALUES (?,?,?)",
                                  DBUtils.VPC.TABLE_NAME,
                                  DBUtils.VPC.ACCOUNT_ID_COLUMN, DBUtils.VPC.NAME_COLUMN ,
                                  DBUtils.VPC.VPC_CREATED_AT);

      Date date = new Date();
      PreparedStatement ps = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
      ps.setInt(1, accountId);
      ps.setString(2, vpc.getVpcName());
      ps.setTimestamp(3, new java.sql.Timestamp(date.getTime()));

      ps.executeUpdate();

      ResultSet result = ps.getGeneratedKeys();
      if (result == null) {
        throw new RuntimeException("Failed Insert");
      }
      result.next();
      return new VPC(result.getInt(1),vpc.getVpcName());
    } catch (SQLException e) {
      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
  }

  @Override
  public boolean removeVPC( int vpcId) throws ConfigurationException, RuntimeException {
    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection= this.poolManager.getConnection();

      String SQL = String.format( "DELETE FROM %s WHERE %s = ?",
                                  DBUtils.VPC.TABLE_NAME,
                                  DBUtils.VPC.VPC_ID_COLUMN);
      PreparedStatement ps = connection.prepareStatement(SQL);

      ps.setInt(1,vpcId);
      ps.executeUpdate();

    } catch (SQLException e) {
      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
    return true;
  }

  @Override
  public boolean addRoles(int accountId, int vpcId, int userId, Role role, String overrides)
           throws ConfigurationException, RuntimeException {

    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection= this.poolManager.getConnection();


      String SQL = String.format( "INSERT INTO %s (%s,%s,%s,%s,%s) VALUES (?,?,?,?,?)",
                                  DBUtils.VPCRole.TABLE_NAME,
                                  DBUtils.VPCRole.VPC_ID_COLUMN, DBUtils.VPCRole.ACCOUNT_ID_COLUMN,
                                  DBUtils.VPCRole.USER_ID_COLUMN, DBUtils.VPCRole.ROLE_TYPE_COLUMN,
                                  DBUtils.VPCRole.ROLE_OVERRIDES_COLUMN);

      PreparedStatement ps = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
      ps.setInt(1, vpcId);
      ps.setInt(2, accountId);
      ps.setInt(3, userId);
      ps.setString(4, role.getRoleType());
      ps.setString(5,overrides);
      ps.executeUpdate();

    }
    catch (SQLException e) {
      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }


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
  public List<VPC> getVPC(int accountId) throws RuntimeException, ConfigurationException {

    List<VPC> vpcList = new ArrayList<VPC>();
    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection = this.poolManager.getConnection();
      String SQL = String.format( "SELECT %s, %s FROM %s WHERE %s = ?",
                                  DBUtils.VPC.VPC_ID_COLUMN, DBUtils.VPC.NAME_COLUMN, //COLUMNS
                                  DBUtils.VPC.TABLE_NAME, //FROM
                                  DBUtils.VPC.ACCOUNT_ID_COLUMN); //WHERE

      PreparedStatement ps = connection.prepareStatement(SQL);
      ps.setInt(1,accountId);
      ResultSet rs = ps.executeQuery();


      while(rs.next()) {
        VPC vpc = new VPC(rs.getInt(1),rs.getString(2));
        vpcList.add(vpc);

      }

    }
    catch (SQLException e) {
      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
    return vpcList;
  }

  @Override
  public List<VPC> getVPC(String apiKey) throws RuntimeException, ConfigurationException {

    List<VPC> vpcList = new ArrayList<VPC>();
    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection = this.poolManager.getConnection();
      String SQL = String.format( "SELECT %s, %s FROM %s JOIN %s ON %s = %s WHERE %s = ?",
                                  DBUtils.VPC.TABLE_NAME+"."+DBUtils.VPC.VPC_ID_COLUMN,
                                  DBUtils.VPC.TABLE_NAME+"."+DBUtils.VPC.NAME_COLUMN, //COLUMNS
                                  DBUtils.VPC.TABLE_NAME, //FROM
                                  DBUtils.AccountTable.TABLE_NAME, //JOIN
                                  DBUtils.VPC.TABLE_NAME+"."+DBUtils.VPC.ACCOUNT_ID_COLUMN, //CONDITION
                                  DBUtils.AccountTable.TABLE_NAME+"."+DBUtils.AccountTable.ID_COLUMN,
                                  DBUtils.AccountTable.TABLE_NAME+"."+DBUtils.AccountTable.API_KEY_COLUMN);

      PreparedStatement ps = connection.prepareStatement(SQL);
      ps.setString(1,apiKey);
      ResultSet rs = ps.executeQuery();

      while(rs.next()) {
        VPC vpc = new VPC(rs.getInt(1),rs.getString(2));
        vpcList.add(vpc);

      }

    }
    catch (SQLException e) {
      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
    return vpcList;
  }
}
