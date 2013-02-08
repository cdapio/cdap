package com.continuuity.passport.common.sql;

import com.continuuity.passport.common.sql.clause.ColumnSelectionClause;
import com.continuuity.passport.common.sql.clause.ExecuteClause;
import com.continuuity.passport.common.sql.clause.InsertColumns;
import com.continuuity.passport.common.sql.clause.QueryClause;
import com.continuuity.passport.common.sql.clause.SQLChain;
import com.continuuity.passport.common.sql.clause.SetClause;
import com.continuuity.passport.common.sql.clause.WhereClause;
import com.continuuity.passport.common.sql.statement.ColumnSelectStatement;
import com.continuuity.passport.common.sql.statement.InsertColumnsStatement;
import com.continuuity.passport.common.sql.statement.SetStatement;
import com.continuuity.passport.common.sql.statement.WhereStatement;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Implements SQLChain for commonly used SQL statements
 */
public class  SQLChainImpl implements SQLChain {

  public static SQLChain getSqlChain (Connection connection) {
    return new SQLChainImpl(connection);
  }

  private Connection connection;

  private SQLChainImpl (Connection connection) {

    this.connection= connection;
  }
  /**
   * Insert statement construction
   * @param table Table Name
   * @return Instance of {@code InsertColumns}
   */
  @Override
  public InsertColumns insert(String table) {
      SQLContext context = new SQLContext(connection, SQLContext.QueryType.INSERT);
      context.getQuery().append("INSERT INTO "+ table + " ");
      InsertColumnsStatement columns = new InsertColumnsStatement();
      columns.setContext(context);
      return columns;
  }

  /**
   * Select queries construction
   * @param table Tablename
   * @return Instance of {@code ColumSelectionClause}
   */
  @Override
  public ColumnSelectionClause<QueryClause<List<Map<String, Object>>>> select(String table) {
    SQLContext context = new SQLContext(connection,SQLContext.QueryType.SELECT);
    context.getQuery().append("SELECT ");
    context.setTable(table);

    ColumnSelectStatement statement = new ColumnSelectStatement();
    statement.setContext(context);
    return statement;

  }

  /**
   * Delete query
   * @param table table name
   * @return
   */
  @Override
  public WhereClause<ExecuteClause> delete(String table){
    SQLContext context = new SQLContext(connection,SQLContext.QueryType.DELETE);
    context.getQuery().append(" DELETE FROM "+table);

    WhereStatement statement = new WhereStatement();
    statement.setContext(context);
    return statement;

  }

  /**
   * Update Statement
   * @param table Table name
   * @return Instance of  {@code SetClause}
   */
  public SetClause<ExecuteClause> update(String table){
    SQLContext context = new SQLContext(connection, SQLContext.QueryType.UPDATE) ;
    context.getQuery().append("UPDATE "+table+ " SET ");

    SetStatement statement = new SetStatement();
    statement.setContext(context);
    return statement;

  }

}
