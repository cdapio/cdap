package com.continuuity.passport.common.sql.clause;

/**
 *  Interface that defines a join operation
 */
public interface JoinClause<T> {

  /**
   * The join() function implements JOIN operation. Note: Only INNER JOIN is supported
   * @return Instance of {@code ConditionClause}
   */
  public ConditionClause<T> joinOn();

}
