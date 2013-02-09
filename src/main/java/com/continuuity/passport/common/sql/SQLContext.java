package com.continuuity.passport.common.sql;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 *  Defines context for query to be executed. Contains JDBC connection, Table name, Query Parameters etc..
 */
public class SQLContext {

  public enum QueryType {SELECT, DELETE, UPDATE, INSERT,SELECT_JOIN}
  private Connection connection;

  private String tableName;

  private StringBuffer query;

  private List<Object> parameters;

  private  QueryType type;

  private String joinTableName;

  public String getJoinTable() {
    return joinTableName;
  }

  public void setJoinTable(String joinTableName) {
    this.joinTableName = joinTableName;
  }

  public QueryType getType() {
    return type;
  }

  SQLContext(Connection connection, QueryType type) {
    this.connection = connection;
    this.type = type;
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
