package com.continuuity.passport.common.sql.clause;

import com.continuuity.passport.common.sql.statement.StatementBase;

/**
 *
 */
public interface ConditionClause<T> {

  public  WhereClause<T> condition(String condition);

}
