package com.continuuity.passport.common.sql;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 *  Defines context for query to be executed. Contains JDBC connection, Table name, Query Parameters etc..
 */
public class SQLContext {

  private Connection connection;

  private String tableName;

  private StringBuffer query;

  private List<Object> parameters;

  SQLContext(Connection connection) {
    this.connection = connection;
    this.parameters = new ArrayList<Object>();
    this.query = new StringBuffer();
  }

  public Connection getConnection() {
    return connection;
  }

  public void setTable(String tableName) {
    this.tableName = tableName;
  }

  public String getTable() {
    return tableName;
  }

  public StringBuffer getQuery() {
    return query;
  }

  public void addParameter(Object value){
    parameters.add(value);
  }

  public List<Object> getParameters() {
    return parameters;
  }


}
