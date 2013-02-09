package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.ConditionClause;
import com.continuuity.passport.common.sql.clause.JoinClause;

/**
 * Implements Join operation. Only INNER JOIN is supported
 */
public class JoinStatement<T> extends StatementBase implements JoinClause {
  /**
   * The join() function implements JOIN operation. Note: Only INNER JOIN is supported
   * @return Instance of {@code ConditionClause}
   */
  @Override
  public ConditionClause<T> joinOn() {
    query().append(table()+" INNER JOIN "+ joinTable()+ " ON ");
    ConditionStatement statement = new ConditionStatement();
    statement.setContext(getContext());
    return statement;
  }
}
