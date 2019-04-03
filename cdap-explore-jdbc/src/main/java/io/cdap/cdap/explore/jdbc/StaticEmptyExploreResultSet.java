/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.explore.jdbc;

import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.proto.ColumnDesc;
import com.google.common.collect.ImmutableList;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Static result set where the schema is set and the results are empty.
 */
public class StaticEmptyExploreResultSet extends BaseExploreResultSet {

  private final List<ImmutablePair<String, String>> schema;

  public StaticEmptyExploreResultSet(List<ImmutablePair<String, String>> schema) {
    this.schema = schema;
  }

  @Override
  public boolean next() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Resultset is closed");
    }
    return false;
  }

  @Override
  public void close() throws SQLException {
    setIsClosed(true);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Resultset is closed");
    }
    ImmutableList.Builder<ColumnDesc> builder = ImmutableList.builder();
    for (int i = 0; i < schema.size(); i++) {
      ImmutablePair<String, String> pair = schema.get(i);
      builder.add(new ColumnDesc(pair.getFirst(), pair.getSecond(), i + 1, ""));
    }
    return new ExploreResultSetMetaData(builder.build());
  }

  @Override
  public Object getObject(int i) throws SQLException {
    if (isClosed()) {
      throw new SQLException("Resultset is closed");
    }
    return null;
  }

  @Override
  public int findColumn(String name) throws SQLException {
    if (isClosed()) {
      throw new SQLException("Resultset is closed");
    }

    for (int i = 0; i < schema.size(); i++) {
      if (schema.get(i).getFirst().equals(name)) {
        return i + 1;
      }
    }
    throw new SQLException("Could not find column with name: " + name);
  }

}
