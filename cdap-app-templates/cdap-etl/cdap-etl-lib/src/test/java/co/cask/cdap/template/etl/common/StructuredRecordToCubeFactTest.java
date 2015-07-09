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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.Measurement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
@SuppressWarnings("unchecked")
public class StructuredRecordToCubeFactTest {

  private static final String DATE_FORMAT = "yyyy:MM:dd-HH:mm:ss Z";

  @Test
  public void testInvalidConfiguration() throws Exception {
    // empty config (at least one measurement must be specified!)
    verifyInvalidConfigDetected(new HashMap<String, String>());

    // bad timestamp
    Map<String, String> config = createValidConfig();
    config.put(Properties.Cube.FACT_TS_FORMAT, "bad-format");
    verifyInvalidConfigDetected(config);

    // bad measurements
    config = createValidConfig();
    config.put(Properties.Cube.MEASUREMENT_PREFIX, "COUNTER");
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    addMeasurement(config, "m1", null);
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    addMeasurement(config, "m1", "bad_type");
    verifyInvalidConfigDetected(config);
  }

  @Test
  public void testTransform() throws Exception {
    // initialize the transform
    StructuredRecordToCubeFact transform = new StructuredRecordToCubeFact(createValidConfig());

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("tsField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("dimField1", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("dimField3", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("boolField", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("intField", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("longField", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("floatField", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("bytesField", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("metricField1", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    // 1. All values are there in fields

    long ts = System.currentTimeMillis();
    StructuredRecord record = StructuredRecord.builder(schema)
        .set("tsField", new SimpleDateFormat(DATE_FORMAT).format(new Date(ts)))
        .set("dimField1", "dimVal1")
        .set("dimField3", "dimVal3")
        .set("boolField", true)
        .set("intField", 5)
        .set("longField", 10L)
        .set("floatField", 3.14f)
        .set("bytesField", Bytes.toBytes("foo"))
        .set("metricField1", "15")
        .build();

    CubeFact transformed = transform.transform(record);
    // note: ts is in seconds
    Assert.assertEquals(ts / 1000, transformed.getTimestamp());

    Map<String, String> dims = transformed.getDimensionValues();
    Assert.assertEquals(9, dims.size());
    Assert.assertEquals(new SimpleDateFormat(DATE_FORMAT).format(new Date(ts)), dims.get("tsField"));
    Assert.assertEquals("dimVal1", dims.get("dimField1"));
    Assert.assertEquals("dimVal3", dims.get("dimField3"));
    Assert.assertEquals("true", dims.get("boolField"));
    Assert.assertEquals("5", dims.get("intField"));
    Assert.assertEquals("10", dims.get("longField"));
    Assert.assertEquals("3.14", dims.get("floatField"));
    Assert.assertEquals(Bytes.toStringBinary(Bytes.toBytes("foo")), dims.get("bytesField"));
    Assert.assertEquals("15", dims.get("metricField1"));

    Collection<Measurement> expectedMeasurements = ImmutableList.of(
      new Measurement("metricField1", MeasureType.COUNTER, 15),
      new Measurement("intField", MeasureType.GAUGE, 5),
      new Measurement("longField", MeasureType.COUNTER, 10),
      new Measurement("floatField", MeasureType.GAUGE, 3)
    );

    verifyMeasurements(expectedMeasurements, transformed.getMeasurements());

    // 2. Verify alternative timestamp retrieval (current ts)

    Map<String, String> config = createValidConfig();
    config.put(Properties.Cube.FACT_TS_FIELD, null);
    transform = new StructuredRecordToCubeFact(config);

    long tsStart = System.currentTimeMillis();
    record = StructuredRecord.builder(schema)
      .set("metricField1", "15")
      .build();

    transformed = transform.transform(record);

    long tsEnd = System.currentTimeMillis();
    // verify that assigned ts was current ts
    Assert.assertTrue(tsStart / 1000 <= transformed.getTimestamp());
    Assert.assertTrue(1000 + tsEnd / 1000 >= transformed.getTimestamp());
  }

  private Map<String, String> createValidConfig() {
    Map<String, String> config = Maps.newHashMap();
    config.put(Properties.Cube.FACT_TS_FIELD, "tsField");
    config.put(Properties.Cube.FACT_TS_FORMAT, DATE_FORMAT);

    addMeasurement(config, "metricField1", "COUNTER");
    addMeasurement(config, "intField", "GAUGE");
    addMeasurement(config, "longField", "COUNTER");
    addMeasurement(config, "floatField", "GAUGE");

    return config;
  }

  private void addMeasurement(Map<String, String> config, String name, String type) {
    config.put(Properties.Cube.MEASUREMENT_PREFIX + name, type);
  }

  private void verifyInvalidConfigDetected(Map<String, String> props) {
    try {
      new StructuredRecordToCubeFact(props);
      Assert.fail("IllegalArgumentException is expected to be thrown on invalid config");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  private void verifyMeasurements(Collection<Measurement> expected, Collection<Measurement> toCompare) {
    Assert.assertEquals(expected.size(), toCompare.size());
    for (Measurement measurement : expected) {
      verifyContainsMeasurement(measurement, toCompare);
    }
  }

  private void verifyContainsMeasurement(Measurement expected, Collection<Measurement> toCompare) {
    for (Measurement measurement : toCompare) {
      if (expected.getName().equals(measurement.getName())) {
        Assert.assertEquals(expected.getType(), measurement.getType());
        Assert.assertEquals(expected.getValue(), measurement.getValue());
        return;
      }
    }
    Assert.fail("Expected measurement not found: " + expected.getName());
  }
}
