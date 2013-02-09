package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.ConditionClause;
import com.continuuity.passport.common.sql.clause.WhereClause;

/**
 *
 */
public class ConditionStatement<T> extends StatementBase implements ConditionClause {

  @Override
  public WhereClause<T> condition(String condition) {
    query().append(" "+condition.toUpperCase()+" ");
    WhereStatement statement = new WhereStatement();
    statement.setContext(getContext());
    return statement;
  }
}
