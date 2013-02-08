package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.ExecuteClause;
import com.continuuity.passport.common.sql.clause.InsertValues;

/**
 * Implements InsertValues to insert Values in the queries
 */
public class InsertValuesStatement extends StatementBase implements InsertValues {

  /**
   * Insert values to the query
   * @param values Object args
   * @return Instance of {@code ExecuteClause}
   */
  @Override
  public ExecuteClause values(Object... values) {
    StringBuffer query = getQuery();
    query.append("VALUES (");

    boolean first =true;

    for (Object value : values ){
      if (first) {
        query.append("? ");
        first=false;
      }
      else {
         query.append(",? ");
      }
      addParameter(value);
    }
    query.append(")");

    ExecuteStatement execute = new ExecuteStatement();
    execute.setContext(getContext());
    return execute;
  }
}
