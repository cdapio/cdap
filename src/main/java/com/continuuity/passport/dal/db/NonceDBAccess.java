package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.utils.NonceUtils;
import com.continuuity.passport.dal.NonceDAO;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import java.sql.*;
import java.util.Map;

/**
 *
 */
public class NonceDBAccess extends DBAccess implements NonceDAO {

  private DBConnectionPoolManager poolManager = null;
  private static final int  SESSION_EXPIRATION_MILLS = 1000 * 60 * 10;
  private static final int  ACTIVATION_EXPIRATION_MILLIS = 1000 * 60 * 60 * 24 * 3;


  public NonceDBAccess(Map<String,String> configurations) {

    String connectionString = configurations.get("connectionString");
    String jdbcType = configurations.get("jdbcType");

    if (jdbcType.toLowerCase().equals("mysql")) {
      MysqlConnectionPoolDataSource mysqlDataSource =  new MysqlConnectionPoolDataSource();
      mysqlDataSource.setUrl(connectionString);
      this.poolManager = new DBConnectionPoolManager(mysqlDataSource, 20);
    }
  }

  @Override
  public int getNonce(int id, NONCE_TYPE type) throws RuntimeException {

    Connection connection= null;
    PreparedStatement ps = null;
    try {
      connection = this.poolManager.getConnection();
      String SQL = String.format( "INSERT INTO %s (%s, %s, %s) VALUES (?,?,?)",
        DBUtils.Nonce.TABLE_NAME,
        DBUtils.Nonce.NONCE_ID_COLUMN,DBUtils.Nonce.ID_COLUMN,DBUtils.Nonce.NONCE_EXPIRES_AT_COLUMN);

      ps = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
      int nonce = NonceUtils.getNonce();
      ps.setInt(1, nonce);
      ps.setInt(2,id);
      if (type.equals(NONCE_TYPE.SESSION)) {
        ps.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()+SESSION_EXPIRATION_MILLS));
      }
      else if (type.equals(NONCE_TYPE.ACTIVATION)) {
        ps.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()+ACTIVATION_EXPIRATION_MILLIS));
      }
      else {
        throw new RuntimeException("Unknown nonce type");
      }
      ps.executeUpdate();

      return nonce;

    }
    catch (SQLException e) {
      throw new RuntimeException(e.getMessage(),e.getCause());
    }
    finally{
      close(connection,ps);
    }

  }

  @Override
  public int getId(int nonce, NONCE_TYPE type) throws RuntimeException, StaleNonceException {

    Connection connection = null;
    PreparedStatement ps = null;
    try {
      connection = this.poolManager.getConnection();
      String SQL = String.format( "SELECT %s, %s FROM %s WHERE %s = ?",
        DBUtils.Nonce.ID_COLUMN,
        DBUtils.Nonce.NONCE_EXPIRES_AT_COLUMN,
        DBUtils.Nonce.TABLE_NAME,
        DBUtils.Nonce.NONCE_ID_COLUMN);

      ps = connection.prepareStatement(SQL);
      ps.setInt(1,nonce);
      ResultSet rs = ps.executeQuery();

      int id = -1;
      int count  = 0;
      while(rs.next()) {
        id = rs.getInt(1);
        Timestamp t = rs.getTimestamp(2);
        if (t.getTime() < System.currentTimeMillis()) {
            throw new StaleNonceException("Older timestamp");
        }

        count++;
        if (count > 1 ) { // Note: This condition should never occur since ids are auto generated.
          throw new RuntimeException("Multiple nonce with same  ID");
        }
      }
      return id;

    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(),e.getCause());
    }
    finally {
      close(connection,ps);
    }
  }
}
