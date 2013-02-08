package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.ColumnSelectionClause;
import com.continuuity.passport.common.sql.clause.WhereClause;

/**
 *
 */
public class ColumnSelectStatement<T> extends StatementBase implements ColumnSelectionClause<T> {


  /**
   * Specify columns to be fetched explicity
   * @param columns column range
   * @return "where" clause
   */
  @Override
  public WhereClause<T> include(String... columns) {
    boolean first = true;
    StringBuffer query = getQuery();

    for(String column: columns){
      if (first){
        query.append(column);
        first =false;
      }
      else{
        query.append(", "+column);
      }
    }

    query.append(" FROM "+table());

    WhereStatement statement = new WhereStatement();
    statement.setContext(getContext());
    return statement;
  }


  /**
   * Include all columns in select statement
   * @return "where" clause
   */
  @Override
  public WhereClause<T> includeAll() {

    StringBuffer query = getQuery();

    query.append(" * FROM " + table());
    WhereStatement statement = new WhereStatement();
    statement.setContext(getContext());
    return statement;
  }
}
