package com.continuuity.passport.dal.db;

import com.mysql.jdbc.jdbc2.optional.PreparedStatementWrapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Abstract class that has some basic operations applicable to db operations
 */
public abstract class DBAccess {

  /**
   * Close DB related objects
   * @param connection DBConnection
   * @param ps   Prepared Statement
   * @param rs   Result set
   * @throws RuntimeException
   */
  public void close(Connection connection, PreparedStatement ps, ResultSet rs) throws RuntimeException{
    try{
      if (connection!=null){
        connection.close();
      }
      if (ps != null) {
        ps.close();
      }
      if (rs != null ){
        rs.close();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(),e.getCause());
    }
  }

  /**
   * Close DB related objects
   * @param connection DBConnection
   * @param ps  Prepared Statement
   * @throws RuntimeException
   */
  public void close(Connection connection, PreparedStatement ps) throws RuntimeException{
    try{
      if (connection!=null){
        connection.close();
      }
      if (ps != null) {
        ps.close();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(),e.getCause());
    }
  }

}
