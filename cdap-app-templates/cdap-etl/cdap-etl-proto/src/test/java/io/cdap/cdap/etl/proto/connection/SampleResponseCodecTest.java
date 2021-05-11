/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.proto.connection;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

/**
 * Test for sample response
 */
public class SampleResponseCodecTest {
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
                                     .registerTypeAdapter(SampleResponse.class, new SampleResponseCodec()).create();

  @Test
  public void testEmptySample() throws Exception {
    SampleResponse sampleResponse = new SampleResponse(
      ConnectorSpec.builder().setProperties(ImmutableMap.of("k1", "v1", "k2", "v2")).build(), null,
      Collections.emptyList());
    String jsonString = GSON.toJson(sampleResponse);
    SampleResponse deserialized = GSON.fromJson(jsonString, SampleResponse.class);
    Assert.assertEquals(sampleResponse, deserialized);
  }

  @Test
  public void testCodec() throws Exception {
    // schema with all types
    Schema schema = Schema.recordOf(
      "schema",
      Schema.Field.of("f1", Schema.of(Schema.Type.INT)),
      Schema.Field.of("f2", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("f3", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("f4", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("f5", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("f6", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("f7", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("f8", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("f9", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("f10", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
      Schema.Field.of("f11", Schema.of(Schema.LogicalType.TIME_MICROS)),
      Schema.Field.of("f12", Schema.of(Schema.LogicalType.TIME_MILLIS)),
      Schema.Field.of("f13", Schema.decimalOf(3, 2)),
      Schema.Field.of("f14", Schema.of(Schema.LogicalType.DATETIME)),
      Schema.Field.of("n1", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("n2", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("n3", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("n4", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("n5", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("n6", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("n7", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("n8", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
      Schema.Field.of("n9", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("n10", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
      Schema.Field.of("n11", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
      Schema.Field.of("n12", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS))),
      Schema.Field.of("n13", Schema.nullableOf(Schema.decimalOf(3, 2))),
      Schema.Field.of("n14", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))));

    // all nullable fields are null
    StructuredRecord record1 =
      StructuredRecord.builder(schema)
        .set("f1", 1).set("f2", "aaa").set("f3", 1L).set("f4", 0d)
        // for bytes and decimals, use bytebuffer to ensure the type is consistent, otherwise the equal compare will
        // fail because of byte[] and bytebuffer comparison
        .set("f5", ByteBuffer.wrap("test".getBytes(Charsets.UTF_8)))
        .set("f6", true).set("f7", 0f).setDate("f8", LocalDate.now()).setTimestamp("f9", ZonedDateTime.now())
        .setTimestamp("f10", ZonedDateTime.now()).setTime("f11", LocalTime.now()).setTime("f12", LocalTime.now())
        .set("f13", ByteBuffer.wrap(new BigDecimal(new BigInteger("111"), 2).unscaledValue().toByteArray()))
        .setDateTime("f14", LocalDateTime.now())
        .build();
    // all fields are filled
    StructuredRecord record2 =
      StructuredRecord.builder(schema)
        .set("f1", 1).set("f2", "aaa").set("f3", 1L).set("f4", 0d)
        .set("f5", ByteBuffer.wrap("test".getBytes(Charsets.UTF_8)))
        .set("f6", true).set("f7", 0f).setDate("f8", LocalDate.now()).setTimestamp("f9", ZonedDateTime.now())
        .setTimestamp("f10", ZonedDateTime.now()).setTime("f11", LocalTime.now()).setTime("f12", LocalTime.now())
        .set("f13", ByteBuffer.wrap(new BigDecimal(new BigInteger("111"), 2).unscaledValue().toByteArray()))
        .setDateTime("f14", LocalDateTime.now())
        .set("n1", 1).set("n2", "aaa").set("n3", 1L).set("n4", 0d)
        .set("n5", ByteBuffer.wrap("test".getBytes(Charsets.UTF_8)))
        .set("n6", true).set("n7", 0f).setDate("n8", LocalDate.now()).setTimestamp("n9", ZonedDateTime.now())
        .setTimestamp("n10", ZonedDateTime.now()).setTime("n11", LocalTime.now()).setTime("n12", LocalTime.now())
        .set("n13", ByteBuffer.wrap(new BigDecimal(new BigInteger("111"), 2).unscaledValue().toByteArray()))
        .setDateTime("n14", LocalDateTime.now())
        .build();
    List<StructuredRecord> sample = ImmutableList.of(record1, record2);
    SampleResponse sampleResponse = new SampleResponse(
      ConnectorSpec.builder().setProperties(ImmutableMap.of("k1", "v1", "k2", "v2")).build(), schema, sample);
    String jsonString = GSON.toJson(sampleResponse);
    SampleResponse deserialized = GSON.fromJson(jsonString, SampleResponse.class);
    Assert.assertEquals(sampleResponse, deserialized);
  }
}
