package com.continuuity.passport.common.sql.clause;

/**
 *  Interface to insert values in an insert statement
 *  INSERT INTO TABLE (COLUMNS) VALUES <b> (Comma separated values)  </b>
 */
public interface InsertValues {

  /**
   * Inserts the values passed in and returns an ExecuteClause
   * @param values Object args
   * @return  Instance of {@code ExecuteClause}
   */
  public ExecuteClause values(Object...values);

}
