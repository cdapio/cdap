package com.continuuity.passport.common.sql.clause;

import java.util.List;
import java.util.Map;

/**
 * Provides functionality to easily execute SQL commands rather than messing with PrepareStatement Query.
 * Example:
 * sql.insert("ACCOUNT").columns("email_id","name").values("sree@continuuity.com","sreevatsan").execute();
 * sql.update("ACCOUNT").set("credit_card","1234 1233 1234 1232").setLast("cvv", "120").execute();
 * sql.delete("ACCOUNT").where("ID").equals("sree@continuuity.com").execute();
 */
public interface SQLChain {

  /**
   * Defines interface to insert table
   * @param table Table Name
   * @return Instance of {@code InsertColumns}
   */
  public InsertColumns insert(String table);

  /**
   * Defines Select queries on the table
   * @param table Tablename
   * @return Instance of {@code ColumSelectionClause}
   */
  public ColumnSelectionClause<QueryClause<List<Map<String, Object>>>> select(String table);
}