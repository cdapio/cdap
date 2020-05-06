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
import org.apache.spark.api.java.function.Function2;

/**
 *
 */
public class SumFunction implements Function2<StructuredRecord, StructuredRecord, StructuredRecord> {

  @Override
  public StructuredRecord call(StructuredRecord v1, StructuredRecord v2) throws Exception {
    StructuredRecord.Builder builder =
      StructuredRecord.builder(Schema.recordOf("output",
                                               Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                               Schema.Field.of("count", Schema.of(Schema.Type.INT)),
                                               Schema.Field.of("sum", Schema.of(Schema.Type.INT))));
    builder.set("id", v1.get("id"));
    Integer count1 = (Integer) v1.get("count");
    Integer count2 = (Integer) v2.get("count");
    builder.set("count", (count1 == null ? 1 : count1) + (count2 == null ? 1 : count2));
    Integer sum1 = (Integer) v1.get("sum");
    Integer sum2 = (Integer) v2.get("sum");
    builder.set("sum", (sum1 == null ? (Integer) v1.get("value") : sum1) +
      (sum2 == null ? (Integer) v2.get("value") : sum2));
    return builder.build();
  }
}
