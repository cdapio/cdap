package com.continuuity.passport.common.sql.clause;

import java.sql.SQLException;

/**
 * Execute queries (SELECT)
 */
public interface QueryClause<T> {

  /**
   * Execute select and result set as T
   * @return ResultSet from the query
   * @throws SQLException
   */
  public T execute() throws SQLException;


}
