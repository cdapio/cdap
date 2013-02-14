package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.common.sql.SQLChain;
import com.continuuity.passport.common.sql.SQLChainImpl;
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
import java.util.List;
import java.util.Map;

/**
 *
 */
public class VpcDBAccess implements VpcDAO {

  private Map<String, String> configuration;

  private DBConnectionPoolManager poolManager =null;

  @Override
  public long addVPC(int accountId, VPC vpc) throws ConfigurationException, RuntimeException {
    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection= this.poolManager.getConnection();
//
      PreparedStatement ps = null;
      String SQL = String.format( "INSERT INTO %s (%s,%s) VALUES (?,?)",
                                  DBUtils.VPC.TABLE_NAME,
                                  DBUtils.VPC.ACCOUNT_ID_COLUMN, DBUtils.VPC.NAME_COLUMN );


      ps = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
      ps.setInt(1, accountId);
      ps.setString(2, vpc.getVpcName());

      ps.executeUpdate();
      ResultSet result = ps.getGeneratedKeys();
      if (result == null) {
        throw new RuntimeException("Failed Insert");
      }
      result.next();
      long id = result.getLong(1);
      return id;
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
      //TODO: Execute in a thread ...
      SQLChain chain =  SQLChainImpl.getSqlChain(connection);
      chain.delete(DBUtils.VPC.TABLE_NAME)
           .where(DBUtils.VPC.VPC_ID_COLUMN).equal(vpcId)
           .execute();
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
      //TODO: Execute in a thread ...
      SQLChain chain =  SQLChainImpl.getSqlChain(connection);
      chain.insert(DBUtils.VPCRole.TABLE_NAME)
           .columns(DBUtils.VPCRole.VPC_ID_COLUMN, DBUtils.VPCRole.ACCOUNT_ID_COLUMN, DBUtils.VPCRole.USER_ID_COLUMN,
                    DBUtils.VPCRole.ROLE_TYPE_COLUMN, DBUtils.VPCRole.ROLE_OVERRIDES_COLUMN)
           .values(vpcId,accountId,userId,role.getRoleId(),overrides).execute();
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
      Connection connection= this.poolManager.getConnection();
      SQLChain chain =  SQLChainImpl.getSqlChain(connection);
      List<Map<String,Object>> resultSet =  chain.select(DBUtils.VPC.TABLE_NAME)
                                                 .includeAll()
                                                 .where(DBUtils.VPC.ACCOUNT_ID_COLUMN).equal(accountId)
                                                 .execute();
      for(Map<String,Object> result : resultSet) {
        VPC vpc = new VPC((Integer)result.get(DBUtils.VPC.VPC_ID_COLUMN),(String)result.get(DBUtils.VPC.NAME_COLUMN));
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
