package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.SQLContext;
import com.continuuity.passport.common.sql.clause.RelationClause;

 /**
  * Specify constraints in the query
  * Only single constraint is supported for now
  */
public class RelationStatement<T> extends StatementBase implements RelationClause {


   /**
    * Specify equals constraints
    * @param value
    * @return T
    */
  @Override
  public T equal(Object value) {
    query().append(" = ?");
    addParameter(value);

    return getStatement();

  }


   /**
    * Specify lessThan constraints
    * @param value
    * @return T
    */
  @Override
  public T lessThan(Object value) {

    query().append(" < ?");
    addParameter(value);

    return getStatement();

  }


   /**
    * Specify greaterThan constraints
    * @param value
    * @return T
    */
  @Override
  public T greaterThan(Object value) {
    query().append(" > ?");
    addParameter(value);

    return getStatement();

  }

   //TODO: MOve this to a helper function
   private T getStatement(){
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
