package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.QueryClause;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Select Statements
 */
public class SelectStatement extends StatementBase
  implements QueryClause<List<Map<String,Object>>> {

  /**
   * Execute query and return resultSet
   * @return ResultSet as List<Map<String,Object>>
   * @throws SQLException
   */
  @Override
  public List<Map<String, Object>> execute() throws SQLException {

    PreparedStatement statement = null;
    ResultSet resultSet = null;
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

    statement = getConnection().prepareStatement(query().toString());

    List<Object> parameters = parameters();

    for( int i =0 ;i< parameters.size();i++) {
      statement.setObject(i+1, parameters.get(i));
    }

    resultSet = statement.executeQuery();

    ResultSetMetaData meta = resultSet.getMetaData();

    while(resultSet.next()) {
      Map<String, Object> entity = new HashMap<String, Object>();
      for (int index = 1; index <= meta.getColumnCount(); index++) {
        entity.put(meta.getColumnName(index), resultSet.getObject(index));
      }

      result.add(entity);

    }
    return result;
  }
}
