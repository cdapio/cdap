package com.continuuity.passport.common.sql.clause;

import java.sql.SQLException;

/**
 *  Interface that can be implemented for executing queries
 */
public interface ExecuteClause {

  /**
   * Execute SQL command and return result
   * @return  true if the first result is a ResultSet object;
   *          false if the first result is an update count or there is no result
   * @throws SQLException
   */
  public boolean execute() throws SQLException;

  }

