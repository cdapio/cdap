package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.InsertColumns;
import com.continuuity.passport.common.sql.clause.InsertValues;

/**
 * Implements InsertColumns
 */
public class InsertColumnsStatement extends StatementBase implements InsertColumns {

  /**
   * Insert columns to queries
   * @param columns columns in insert statement
   * @return Instance of {@code InsertValues}
   */
  @Override
  public InsertValues columns(String...columns) {

    StringBuffer query = getQuery();
    query.append(" (");

    boolean first =true;

    for (String column :  columns) {
      if(first) {
        query.append(column);
        first = false;
      }
      else {
        query.append(","+column);
      }
    }

    query.append(")");

    InsertValuesStatement values = new InsertValuesStatement();
    values.setContext(getContext());
    return values;

  }
}
