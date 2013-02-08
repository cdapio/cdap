package com.continuuity.passport.common.sql.statement;

import com.continuuity.passport.common.sql.clause.RelationClause;

 /**
  * Specify constraints in the query
  * Only single constraint is supported for now
  */
public class RelationStatement extends StatementBase implements RelationClause {


   /**
    * Specify equals constraints
    * @param value
    * @return T
    */
  @Override
  public Object equal(Object value) {
    query().append(" = ?");
    addParameter(value);

    SelectStatement statement = new SelectStatement();
    statement.setContext(getContext());
    return statement;
  }


   /**
    * Specify lessThan constraints
    * @param value
    * @return T
    */
  @Override
  public Object lessThan(Object value) {

    query().append(" < ?");
    addParameter(value);

    SelectStatement statement = new SelectStatement();
    statement.setContext(getContext());
    return statement;
  }


   /**
    * Specify greaterThan constraints
    * @param value
    * @return T
    */
  @Override
  public Object greaterThan(Object value) {
    query().append(" > ?");
    addParameter(value);

    SelectStatement statement = new SelectStatement();
    statement.setContext(getContext());
    return statement;
  }
}
