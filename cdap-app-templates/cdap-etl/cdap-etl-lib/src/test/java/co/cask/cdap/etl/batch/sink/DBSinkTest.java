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

package co.cask.cdap.etl.batch.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.common.MockEmitter;
import co.cask.cdap.etl.common.db.DBRecord;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class DBSinkTest {

  @Test
  public void testIdentity() throws Exception {
    final DBSink.DBSinkConfig config = new DBSink.DBSinkConfig();
    config.columns = "ts,body";
    config.tableName = "foo";

    DBSink sink = new DBSink(config) {
      @Override
      public List<String> getColumns() {
        return ImmutableList.copyOf(Splitter.on(",").split(config.columns));
      }
    };

    StructuredRecord input = StructuredRecord
      .builder(Schema.recordOf(
        "foo",
        Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("body", Schema.of(Schema.Type.STRING))))
      .set("ts", 123)
      .set("body", "sdfsdf")
      .build();

    MockEmitter<KeyValue<DBRecord, NullWritable>> emitter = new MockEmitter<>();
    sink.transform(input, emitter);

    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals(
      getRecordFields(input),
      getRecordFields(emitter.getEmitted().get(0).getKey().getRecord()));
  }

  @Test
  public void testDropColumn() throws Exception {
    final DBSink.DBSinkConfig config = new DBSink.DBSinkConfig();
    config.columns = "body,ts";
    config.tableName = "foo";

    DBSink sink = new DBSink(config) {
      @Override
      public List<String> getColumns() {
        return ImmutableList.copyOf(Splitter.on(",").split(config.columns));
      }
    };

    StructuredRecord input = StructuredRecord
      .builder(Schema.recordOf(
        "foo",
        Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
        Schema.Field.of("body", Schema.of(Schema.Type.STRING))))
      .set("ts", 123)
      .set("headers", ImmutableMap.of("hello", "zzz"))
      .set("body", "sdfsdf")
      .build();

    StructuredRecord output = StructuredRecord
      .builder(Schema.recordOf(
        "foo",
        Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("ts", Schema.of(Schema.Type.LONG))))
      .set("body", "sdfsdf")
      .set("ts", 123)
      .build();

    MockEmitter<KeyValue<DBRecord, NullWritable>> emitter = new MockEmitter<>();
    sink.transform(input, emitter);

    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals(
      getRecordFields(output),
      getRecordFields(emitter.getEmitted().get(0).getKey().getRecord()));
  }

  @Test
  public void testFailure() throws Exception {
    final DBSink.DBSinkConfig config = new DBSink.DBSinkConfig();
    config.columns = "body,ts,missing";
    config.tableName = "foo";

    DBSink sink = new DBSink(config) {
      @Override
      public List<String> getColumns() {
        return ImmutableList.copyOf(Splitter.on(",").split(config.columns));
      }
    };

    StructuredRecord input = StructuredRecord
      .builder(Schema.recordOf(
        "foo",
        Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
        Schema.Field.of("body", Schema.of(Schema.Type.STRING))))
      .set("ts", 123)
      .set("headers", ImmutableMap.of("hello", "zzz"))
      .set("body", "sdfsdf")
      .build();

    MockEmitter<KeyValue<DBRecord, NullWritable>> emitter = new MockEmitter<>();
    try {
      sink.transform(input, emitter);
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
  }

  public Map<String, Object> getRecordFields(StructuredRecord record) {
    Map<String, Object> fields = Maps.newHashMap();
    for (Schema.Field field : record.getSchema().getFields()) {
      fields.put(field.getName(), record.get(field.getName()));
    }
    return fields;
  }
}
