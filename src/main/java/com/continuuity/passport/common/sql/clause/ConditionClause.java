package com.continuuity.passport.common.sql.clause;

import com.continuuity.passport.common.sql.statement.StatementBase;

/**
 *  Add a condition to select statement
 */
public interface ConditionClause<T> {

  /**
   * Add condition
   * @param condition condition - Note: conditions of the form A = B is the only supported type
   *                  Example condition("TABLE1.COL1 = TABLE2.COL2");
   *
   * @return Instance of {@code WhereClause} to add additional constraints
   *
   */
  public  WhereClause<T> condition(String condition);

}
