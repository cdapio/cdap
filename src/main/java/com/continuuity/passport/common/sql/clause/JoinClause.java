package com.continuuity.passport.common.sql.clause;

/**
 *
 */
public interface JoinClause<T> {

  public ConditionClause<T> joinOn();

}
