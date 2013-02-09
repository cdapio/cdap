package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.ConditionClause;
import com.continuuity.passport.common.sql.clause.WhereClause;

/**
 *  Add condition
 */
public class ConditionStatement<T> extends StatementBase implements ConditionClause {

  /**
   * Add condition
   * @param condition condition - Note: conditions of the form A = B is the only supported type
   *                  Example condition("TABLE1.COL1 = TABLE2.COL2");
   *
   * @return Instance of {@code WhereClause} to add additional constraints
   *
   */
  @Override
  public WhereClause<T> condition(String condition) {
    query().append(" "+condition.toUpperCase()+" ");
    WhereStatement statement = new WhereStatement();
    statement.setContext(getContext());
    return statement;
  }
}
