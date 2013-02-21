/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import com.google.common.base.Throwables;

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
   *
   * @param connection DBConnection
   * @param ps         Prepared Statement
   * @param rs         Result set
   * @throws RuntimeException
   */
  public void close(Connection connection, PreparedStatement ps, ResultSet rs) {
    try {
      if (connection != null) {
        connection.close();
      }
      if (ps != null) {
        ps.close();
      }
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Close DB related objects
   *
   * @param connection DBConnection
   * @param ps         Prepared Statement
   * @throws RuntimeException
   */
  public void close(Connection connection, PreparedStatement ps) {
    try {
      if (connection != null) {
        connection.close();
      }
      if (ps != null) {
        ps.close();
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }

}
