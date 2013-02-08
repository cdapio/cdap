package com.continuuity.passport.common.sql.clause;

/**
 * Specify constraints in the query
 * Only single constraint is supported for now
 */
public interface RelationClause<T> {

  /**
   * Specify equals constraints
   * @param value
   * @return T
   */
  public T equal(Object value);

  /**
   * Specify less than constraints
   * @param value
   * @return T
   */
  public T lessThan(Object value);

  /**
   * Specify greater than constraints
   * @param value
   * @return T
   */
  public T greaterThan(Object value);

}