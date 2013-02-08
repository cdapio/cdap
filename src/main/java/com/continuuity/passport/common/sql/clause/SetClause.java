package com.continuuity.passport.common.sql.clause;

/**
 *
 */
public interface SetClause<T> {

  public SetClause<T> set(String column, Object value);

  public WhereClause<T> setLast(String column, Object value);

}
