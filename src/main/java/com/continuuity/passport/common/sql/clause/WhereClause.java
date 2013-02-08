package com.continuuity.passport.common.sql.clause;

/**
 * Specify where clause
 */
public interface WhereClause<T> {


  /**
   * Return RelationClause to specify additional Constraint
   * @param column
   * @return Instance of {@code RelationClause}
   */
  public RelationClause<T> where(String column);

  /**
   * Specifies No where clause in the query - return everything
   * @return   T
   */
  public T noWhere();

}
