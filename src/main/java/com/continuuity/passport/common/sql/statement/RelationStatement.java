package com.continuuity.passport.common.sql.statement;

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
}
