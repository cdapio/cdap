package com.continuuity.passport.common.sql.clause;

/**
 *  Interface to insert columns in an insert statement
 *  INSERT INTO TABLE <b> (COLUMNS) </b> VALUES (Comma separated values)
 */
public interface InsertColumns {

  /**
   * Append columns to queries
   * @param values String args that defines columns
   * @return Instance of {@code InsertValues} that defines interface to insert the values
   */
  public InsertValues columns(String...values);

}
