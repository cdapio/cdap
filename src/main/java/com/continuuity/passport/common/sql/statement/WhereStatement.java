package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.RelationClause;
import com.continuuity.passport.common.sql.clause.WhereClause;

/**
 * Specify Where clause
 */
public class WhereStatement<T> extends StatementBase implements WhereClause<T> {

  /**
   * Return RelationClause to specify additional Constraint
   * @param column
   * @return Instance of {@code RelationClause}
   */
  @Override
  public RelationClause<T> where(String column) {
    query().append(" WHERE "+ column);
    RelationStatement statement = new RelationStatement();
    statement.setContext(getContext());
    return statement;
  }

  /**
   * Specifies No where clause in the query - return everything
   * @return   T
   */
  @Override
  public T noWhere() {

    return getStatement();
  }

}
