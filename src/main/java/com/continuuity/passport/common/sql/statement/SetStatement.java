package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.SetClause;
import com.continuuity.passport.common.sql.clause.WhereClause;

/**
 *  SetStatement used in conjunction with Update Statement
 */
public class SetStatement<T> extends StatementBase implements SetClause<T>{

  /**
   * Set a column and value
   * @param column column name
   * @param value  value
   * @return Instance of {@code SetClause}
   */
  @Override
  public SetClause<T> set(String column, Object value) {
    StringBuffer query = query();
    //Check if we are adding first Column or intermediate columns to put commas appropriately
    if (query.toString().trim().endsWith("SET")) {
      query.append(" "+column +" = ? ");
    }
    else {
      query.append(", "+column +" = ? ");
    }
    addParameter(value);

    SetStatement statement = new SetStatement();
    statement.setContext(getContext());
    return statement;

  }

  /**
   * Set the last Column to include where clauses
   * @param column column name in the table
   * @param value value to be set
   * @return Instance of {@code WhereClause}
   */
  @Override
  public WhereClause<T> setLast(String column, Object value) {

    //Check if we are adding first Column or intermediate columns to put commas appropriately
    StringBuffer query = query();
    if (query.toString().trim().endsWith("SET")) {
      query.append(" "+column +" = ? ");
    }
    else {
      query.append(", "+column +" = ? ");
    }
    addParameter(value);
    WhereStatement statement = new WhereStatement();
    statement.setContext(getContext());
    return statement;
  }
}
