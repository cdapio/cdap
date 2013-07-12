/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Abstract class that has some basic operations applicable to db operations.
 */
public abstract class DBAccess {

  private static final Logger LOG = LoggerFactory.getLogger(DBAccess.class);

  /**
   * Close DB related objects.
   * @param connection DBConnection
   * @param ps         Prepared Statement
   * @param rs         Result set
   */
  public void close(Connection connection, PreparedStatement ps, ResultSet rs) {

    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      LOG.error(String.format("Error while closing Result set %s", e.getMessage()));
    }
    try {
      if (ps != null) {
        ps.close();
      }
    } catch (SQLException e) {
      LOG.error(String.format("Error while closing PreparedStatement set %s", e.getMessage()));
    }
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOG.error(String.format("Error while closing Connection set %s", e.getMessage()));
    }
  }

  /**
   * Close DB related objects.
   * @param connection DBConnection
   * @param ps         Prepared Statement
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

  /**
   * Close DB related objects.
   * @param ps Prepared Statement
   */
  public void close(PreparedStatement ps) {
    try {
      if (ps != null) {
        ps.close();
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Close connection.
   * @param connection DBConnection to be closed.
   */
  public void close(Connection connection) {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Close ResultSet.
   * @param rs ResultSet.
   */
  public void close(ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }
}
