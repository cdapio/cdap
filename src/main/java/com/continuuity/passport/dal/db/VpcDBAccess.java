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
import java.sql.SQLException;
import java.util.Map;

/**
 *
 */
public class VpcDBAccess implements VpcDAO {

  private Map<String, String> configuration;

  private DBConnectionPoolManager poolManager =null;

  @Override
  public boolean addVPC(int accountId, VPC vpc) throws ConfigurationException, RuntimeException {
    if (this.poolManager == null){
      throw new ConfigurationException("DBConnection pool is null. DAO is not configured");
    }
    try {
      Connection connection= this.poolManager.getConnection();
      //TODO: Execute in a thread ...
      SQLChain chain =  SQLChainImpl.getSqlChain(connection);
      chain.insert(Common.VPC.TABLE_NAME)
        .columns(Common.VPC.ACCOUNT_ID_COLUMN,Common.VPC.NAME_COLUMN)
        .values(accountId, vpc.getVpcName())
        .execute();
    } catch (SQLException e) {
      //TODO: Log
      throw new RuntimeException(e.getMessage(), e.getCause());
    }
    return true;
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
      chain.delete(Common.VPC.TABLE_NAME)
           .where(Common.VPC.VPC_ID_COLUMN).equal(vpcId)
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
      chain.insert(Common.VPCRole.TABLE_NAME)
           .columns(Common.VPCRole.VPC_ID_COLUMN,Common.VPCRole.ACCOUNT_ID_COLUMN,Common.VPCRole.USER_ID,
                    Common.VPCRole.ROLE_TYPE_COLUMN,Common.VPCRole.ROLE_OVERRIDES_COLUMN)
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
}
