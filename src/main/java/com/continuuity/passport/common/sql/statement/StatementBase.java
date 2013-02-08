package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.SQLContext;

import java.sql.Connection;
import java.util.List;

/**
 * Abstract clause that is used to extend all statements
 */
public abstract class StatementBase {

  private SQLContext sqlContext;

  public SQLContext getContext() {
    return sqlContext;
  }

  public void setContext(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  public StringBuffer query() {
    return sqlContext.getQuery();
  }

  public void addParameter(Object value){
    sqlContext.addParameter(value);
  }

  public Connection getConnection() {
    return sqlContext.getConnection();
  }
  public List<Object> parameters() {
    return sqlContext.getParameters();
  }

  public StringBuffer getQuery(){
    return sqlContext.getQuery();
  }

  public String table(){
    return sqlContext.getTable();
  }

  public SQLContext.QueryType getType() {
    return sqlContext.getType();
  }

  public <T> T getStatement(){
    StatementBase statement = null;

    if (getType().equals(SQLContext.QueryType.SELECT)) {
      statement = new SelectStatement();
      statement.setContext(getContext());
    }
    else if (getType().equals(SQLContext.QueryType.DELETE)){
      statement = new ExecuteStatement() ;
      statement.setContext(getContext());
    }

    return (T) statement;
  }
}
