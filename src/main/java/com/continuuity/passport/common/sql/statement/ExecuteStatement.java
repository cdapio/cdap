package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.ExecuteClause;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Implements Execute clause use to execute queries
 */
public class ExecuteStatement extends StatementBase implements ExecuteClause {

  /**
   * Execute SQL queries
   * @return boolean true on successful query execution
   * @throws SQLException
   */
  @Override
  public boolean run() throws SQLException {
    PreparedStatement statement = null;
    try{
      statement = getConnection().prepareStatement(getQuery().toString());
      List<Object> parameters = parameters();

      for( int i =0 ;i< parameters.size();i++) {
        statement.setObject(i+1, parameters.get(i));
      }
      return statement.execute();
    }
    finally {
      if (statement != null) {
        statement.close();
      }
    }
  }
}
