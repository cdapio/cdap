/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.function;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.RecordType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class HardCode implements FlatMapFunc<Tuple2<Object, StructuredRecord>, RecordInfo<Object>> {
  private final PluginFunctionContext pluginFunctionContext;

  public HardCode(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<RecordInfo<Object>> call(Tuple2<Object, StructuredRecord> value) throws Exception {
    List<RecordInfo<Object>> object = new ArrayList<>();
    StructuredRecord.Builder builder =
      StructuredRecord.builder(Schema.recordOf("output",
                                               Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                               Schema.Field.of("avg", Schema.of(Schema.Type.INT)),
                                               Schema.Field.of("sum", Schema.of(Schema.Type.INT)),
                                               Schema.Field.of("count", Schema.of(Schema.Type.INT))));
    StructuredRecord result = value._2;
    builder.set("id", result.get("id"));
    builder.set("sum", result.get("sum"));
    builder.set("count", result.get("count"));
    builder.set("avg", (Integer) result.get("sum") / (Integer) result.get("count"));
    object.add(RecordInfo.<Object>builder(builder.build(), pluginFunctionContext.getStageName(),
                                          RecordType.OUTPUT).build());
    return object;
  }
}
