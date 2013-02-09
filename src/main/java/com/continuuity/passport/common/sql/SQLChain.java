package com.continuuity.passport.common.sql;

import com.continuuity.passport.common.sql.clause.ColumnSelectionClause;
import com.continuuity.passport.common.sql.clause.ExecuteClause;
import com.continuuity.passport.common.sql.clause.InsertColumns;
import com.continuuity.passport.common.sql.clause.JoinClause;
import com.continuuity.passport.common.sql.clause.QueryClause;
import com.continuuity.passport.common.sql.clause.SetClause;
import com.continuuity.passport.common.sql.clause.WhereClause;

import java.util.List;
import java.util.Map;

/**
 * Provides functionality to easily execute SQL commands rather than messing with PrepareStatement Query.
 * Example:
 * sql.insert("ACCOUNT").columns("email_id","name").values("sree@continuuity.com","sreevatsan").execute();
 * sql.update("ACCOUNT").set("credit_card","1234 1233 1234 1232").setLast("cvv", "120").execute();
 * sql.delete("ACCOUNT").where("ID").equals("sree@continuuity.com").execute();
 * sql.select("ACCOUNT").columns("email_id").where("ID").equals("123").execute();
 * sql.selectJoin("ACCOUNT","VPC").columns("email_id").where("id").equals("123").join().on("VPC").condition().execute();
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

  /**
   * DELETE statement
   * @param table table name
   * @return Instance of {@code WhereClause}
   */
  public WhereClause<ExecuteClause> delete(String table);


  /**
   * Update statement
   * @param table Table name
   * @return Instance of {@code SetClause}
   */
  public SetClause<ExecuteClause> update(String table);


  /**
   * Simple join on two tables. Does inner join
   * @param table1 Table name
   * @return Instance of {@code ColumnSelectionClause}
   */
  public JoinClause<QueryClause<List<Map<String, Object>>>> selectWithJoin(String table1,String table2);


}

