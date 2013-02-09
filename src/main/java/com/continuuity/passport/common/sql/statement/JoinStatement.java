package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.ConditionClause;
import com.continuuity.passport.common.sql.clause.JoinClause;

/**
 *
 */
public class JoinStatement<T> extends StatementBase implements JoinClause {

  @Override
  public ConditionClause<T> joinOn() {
    query().append(table()+" INNER JOIN "+ joinTable()+ " ON ");
    ConditionStatement statement = new ConditionStatement();
    statement.setContext(getContext());
    return statement;
  }
}
