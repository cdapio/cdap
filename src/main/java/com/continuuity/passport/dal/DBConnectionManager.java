package com.continuuity.passport.dal;

import com.continuuity.common.db.DBConnectionPoolManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;


/**
 * Singleton instance that manages connection pool
 */

public class DBConnectionManager {

  private DBConnectionPoolManager poolManager ;

  private DBConnectionManager(Map<String, String> config) {
  }

  public synchronized Connection getConnection() throws SQLException {
    if(poolManager != null) {
      return poolManager.getValidConnection();
    }
    return null;
  }


  DBConnectionManager getInstance () {
    return null; //TODO: Add single instance creation
  }
}
