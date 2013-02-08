package com.continuuity.passport.common.sql.clause;

/**
 * Select Columns for SELECT statements
 */
public interface ColumnSelectionClause<T> {

  /**
   * Specify columns to be fetched explicity
   * @param columns column range
   * @return "where" clause
   */
  WhereClause<T> include(String... columns);

  /**
   * Include all columns in select statement
   * @return "where" clause
   */
  WhereClause<T> includeAll();

}
