package com.continuuity.explore.jdbc.meta;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @param <M>
 */
public abstract class ExploreMetaDataResultSet<M> extends ExploreBaseResultSet {
  protected final List<M> data;

  @SuppressWarnings("unchecked")
  public ExploreMetaDataResultSet(final List<String> columnNames
    , final List<String> columnTypes
    , final List<M> data) throws SQLException {
    if (data != null) {
      this.data = new ArrayList<M>(data);
    } else {
      this.data =  new ArrayList<M>();
    }
    if (columnNames != null) {
      this.columnNames = new ArrayList<String>(columnNames);
    } else {
      this.columnNames =  new ArrayList<String>();
    }
    if (columnTypes != null) {
      this.columnTypes = new ArrayList<String>(columnTypes);
    } else {
      this.columnTypes =  new ArrayList<String>();
    }
  }

  @Override
  public void close() throws SQLException {
  }

}
