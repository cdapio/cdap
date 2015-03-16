/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.lib.sources.realtime;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.templates.etl.api.SourceContext;
import co.cask.cdap.templates.etl.api.realtime.Emitter;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.api.stages.AbstractRealtimeSource;
import com.mysql.jdbc.Driver;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nullable;

/**
 *
 */
public class MySQLSource extends AbstractRealtimeSource<GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLSource.class);

  private Connection connect;
  private Statement statement;
  private ResultSet resultSet;
  private Driver driver;
  private String schemaString = Schema.recordOf("HRRecord", Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                                Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("age", Schema.of(Schema.Type.INT))).toString();
  private org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(schemaString);

  @Override
  public void initialize(SourceContext context) {
    super.initialize(context);
    try {
      Class.forName("com.mysql.jdbc.Driver");
      connect = DriverManager.getConnection("jdbc:mysql://localhost/test?" + "user=root");
      statement = connect.createStatement();
      resultSet = statement.executeQuery("select * from hr");
    } catch (ClassNotFoundException e) {
      LOG.error(e.getMessage(), e);
    } catch (SQLException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<GenericRecord> writer, @Nullable SourceState currentState) {
    try {
      if (currentState != null && currentState.getState("offset") != null) {
        int off = Bytes.toInt(currentState.getState("offset"));
        resultSet.absolute(off);
      }

      if (resultSet != null && resultSet.next()) {
        writer.emit(
          new GenericRecordBuilder(SCHEMA)
            .set("id", resultSet.getInt("id"))
            .set("name", resultSet.getString("name"))
            .set("age", resultSet.getInt("age")).build());

        SourceState newState = currentState;
        if (currentState == null) {
          newState = new SourceState();
        }
        newState.setState("offset", Bytes.toBytes(resultSet.getRow()));
        return newState;
      }
    } catch (SQLException e) {
      LOG.error(e.getMessage(), e);
    }
    return currentState;
  }

  @Override
  public void destroy() {
    try {
      resultSet.close();
    } catch (SQLException e) {
      LOG.error(e.getMessage(), e);
    }
  }
}
