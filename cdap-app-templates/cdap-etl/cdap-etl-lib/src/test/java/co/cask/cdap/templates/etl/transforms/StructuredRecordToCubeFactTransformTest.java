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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.Measurement;
import co.cask.cdap.templates.etl.api.StageContext;
import co.cask.cdap.templates.etl.api.TransformStage;
import co.cask.cdap.templates.etl.common.MockEmitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
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
public class StructuredRecordToCubeFactTransformTest {

  private static final String DATE_FORMAT = "yyyy:MM:dd-HH:mm:ss Z";

  @Test
  public void testInvalidConfiguration() throws Exception {
    // no config
    verifyInvalidConfigDetected(null);
    // empty
    verifyInvalidConfigDetected(new StructuredRecordToCubeFactTransform.MappingConfig());

    // bad timestamp
    StructuredRecordToCubeFactTransform.MappingConfig config = createValidConfig();
    config.timestamp = null;
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    config.timestamp.sourceField = null;
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    config.timestamp.sourceField = null;
    config.timestamp.value = "not_now";
    verifyInvalidConfigDetected(config);

    // bad tags
    config = createValidConfig();
    config.tags = null;
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    config.tags[0].name = null;
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    config.tags[0].sourceField = null;
    verifyInvalidConfigDetected(config);

    // bad measurements
    config = createValidConfig();
    config.measurements = null;
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    config.measurements[0].name = null;
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    config.measurements[0].type = null;
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    config.measurements[0].sourceField = null;
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    config.measurements[0].sourceField = null;
    config.measurements[0].value = "not_long";
    verifyInvalidConfigDetected(config);

    config = createValidConfig();
    config.measurements[0].value = "not_long";
    verifyInvalidConfigDetected(config);
  }

  @Test
  public void testTransform() throws Exception {
    // initialize the transform
    TransformStage transform = new StructuredRecordToCubeFactTransform();
    StageContext context = new MockTransformContext(
      ImmutableMap.of(StructuredRecordToCubeFactTransform.MAPPING_CONFIG_PROPERTY,
                      new Gson().toJson(createValidConfig())));
    transform.initialize(context);

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("tsField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("tagField1", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("tagField3", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
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
        .set("tagField1", "tagVal1")
        .set("tagField3", "tagVal3")
        .set("boolField", true)
        .set("intField", 5)
        .set("longField", 10L)
        .set("floatField", 3.14f)
        .set("bytesField", Bytes.toBytes("foo"))
        .set("metricField1", "15")
        .build();

    MockEmitter<CubeFact> emitter = new MockEmitter<CubeFact>();
    transform.transform(record, emitter);

    CubeFact transformed = emitter.getEmitted().get(0);
    // note: ts is in seconds
    Assert.assertEquals(ts / 1000, transformed.getTimestamp());

    Map<String, String> tags = transformed.getTags();
    Assert.assertEquals(7, tags.size());
    Assert.assertEquals("tagVal1", tags.get("tag1"));
    Assert.assertEquals("value2", tags.get("tag2"));
    Assert.assertEquals("tagVal3", tags.get("tag3"));
    Assert.assertEquals("true", tags.get("boolTag"));
    Assert.assertEquals("5", tags.get("intTag"));
    Assert.assertTrue(Math.abs(3.14f - Float.valueOf(tags.get("floatTag"))) < 0.000001);
    Assert.assertEquals(Bytes.toStringBinary(Bytes.toBytes("foo")), tags.get("bytesTag"));

    Collection<Measurement> expectedMeasurements = ImmutableList.of(
      new Measurement("metric1", MeasureType.COUNTER, 15),
      new Measurement("metric2", MeasureType.COUNTER, 55),
      new Measurement("intMetric", MeasureType.GAUGE, 5),
      new Measurement("longMetric", MeasureType.COUNTER, 10),
      new Measurement("floatMetric", MeasureType.GAUGE, 3)
    );

    verifyMeasurements(expectedMeasurements, transformed.getMeasurements());

    // 2. Not all values are there, validate fallback to defaults and alternative timestamp retrieval

    transform = new StructuredRecordToCubeFactTransform();
    StructuredRecordToCubeFactTransform.MappingConfig config = createValidConfig();
    config.timestamp.sourceField = null;
    config.timestamp.sourceFieldFormat = null;
    config.timestamp.value = "now";
    context = new MockTransformContext(ImmutableMap.of(StructuredRecordToCubeFactTransform.MAPPING_CONFIG_PROPERTY,
                                                       new Gson().toJson(config)));
    transform.initialize(context);

    long tsStart = System.currentTimeMillis();
    record = StructuredRecord.builder(schema)
      .set("tsField", new SimpleDateFormat(DATE_FORMAT).format(new Date(ts)))
      .set("tagField1", "tagVal1")
      .set("boolField", true)
      .set("longField", 10L)
      .set("floatField", 3.14f)
      .set("bytesField", Bytes.toBytes("foo"))
      .set("metricField1", "15")
      .build();

    emitter = new MockEmitter<CubeFact>();
    transform.transform(record, emitter);

    long tsEnd = System.currentTimeMillis();

    transformed = emitter.getEmitted().get(0);
    // verify that assigned ts was current ts
    Assert.assertTrue(tsStart / 1000 <= transformed.getTimestamp());
    Assert.assertTrue(1000 + tsEnd / 1000 >= transformed.getTimestamp());

    tags = transformed.getTags();
    Assert.assertEquals(6, tags.size());
    Assert.assertEquals("tagVal1", tags.get("tag1"));
    Assert.assertEquals("value2", tags.get("tag2"));
    Assert.assertEquals("defaultTagVal3", tags.get("tag3"));
    Assert.assertEquals("true", tags.get("boolTag"));
    Assert.assertTrue(Math.abs(3.14f - Float.valueOf(tags.get("floatTag"))) < 0.000001);
    Assert.assertEquals(Bytes.toStringBinary(Bytes.toBytes("foo")), tags.get("bytesTag"));

    expectedMeasurements = ImmutableList.of(
      new Measurement("metric1", MeasureType.COUNTER, 15),
      new Measurement("metric2", MeasureType.COUNTER, 55),
      new Measurement("intMetric", MeasureType.GAUGE, 66),
      new Measurement("longMetric", MeasureType.COUNTER, 10),
      new Measurement("floatMetric", MeasureType.GAUGE, 3)
    );

    verifyMeasurements(expectedMeasurements, transformed.getMeasurements());

  }

  private StructuredRecordToCubeFactTransform.MappingConfig createValidConfig() {
    StructuredRecordToCubeFactTransform.MappingConfig config = new StructuredRecordToCubeFactTransform.MappingConfig();
    config.timestamp = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.timestamp.sourceField = "tsField";
    config.timestamp.sourceFieldFormat = DATE_FORMAT;

    config.tags = new StructuredRecordToCubeFactTransform.ValueMapping[7];
    config.tags[0] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.tags[0].name = "tag1";
    config.tags[0].sourceField = "tagField1";

    config.tags[1] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.tags[1].name = "tag2";
    config.tags[1].value = "value2";

    config.tags[2] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.tags[2].name = "tag3";
    config.tags[2].sourceField = "tagField3";
    config.tags[2].value = "defaultTagVal3";

    config.tags[3] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.tags[3].name = "boolTag";
    config.tags[3].sourceField = "boolField";

    config.tags[4] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.tags[4].name = "intTag";
    config.tags[4].sourceField = "intField";

    config.tags[5] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.tags[5].name = "floatTag";
    config.tags[5].sourceField = "floatField";

    config.tags[6] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.tags[6].name = "bytesTag";
    config.tags[6].sourceField = "bytesField";

    config.measurements = new StructuredRecordToCubeFactTransform.ValueMapping[5];
    config.measurements[0] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.measurements[0].name = "metric1";
    config.measurements[0].type = "COUNTER";
    config.measurements[0].sourceField = "metricField1";

    config.measurements[1] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.measurements[1].name = "metric2";
    config.measurements[1].type = "COUNTER";
    config.measurements[1].value = "55";

    config.measurements[2] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.measurements[2].name = "intMetric";
    config.measurements[2].type = "GAUGE";
    config.measurements[2].sourceField = "intField";
    config.measurements[2].value = "66";

    config.measurements[3] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.measurements[3].name = "longMetric";
    config.measurements[3].type = "COUNTER";
    config.measurements[3].sourceField = "longField";

    config.measurements[4] = new StructuredRecordToCubeFactTransform.ValueMapping();
    config.measurements[4].name = "floatMetric";
    config.measurements[4].type = "GAUGE";
    config.measurements[4].sourceField = "floatField";

    return config;
  }

  private void verifyInvalidConfigDetected(StructuredRecordToCubeFactTransform.MappingConfig config) {
    TransformStage transform = new StructuredRecordToCubeFactTransform();
    StageContext context =
      new MockTransformContext(config == null ? new HashMap<String, String>() :
                                 ImmutableMap.of(StructuredRecordToCubeFactTransform.MAPPING_CONFIG_PROPERTY,
                                                 new Gson().toJson(config)));
    try {
      transform.initialize(context);
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
