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

package co.cask.cdap.etl.transform;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.common.MockEmitter;
import co.cask.cdap.etl.common.MockMetrics;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 */
@SuppressWarnings("unchecked")
public class ScriptFilterTransformTest {

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidScript() throws Exception {
    ScriptFilterTransform.ScriptFilterConfig config = new ScriptFilterTransform.ScriptFilterConfig();
    config.script = "funtion() { return false; }";
    Transform transform = new ScriptFilterTransform(config);
    TransformContext transformContext = new MockTransformContext(ImmutableMap.<String, String>of());
    transform.initialize(transformContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFilter() throws Exception {
    ScriptFilterTransform.ScriptFilterConfig config = new ScriptFilterTransform.ScriptFilterConfig();
    config.script =
      "function shouldFilter(input, context) { context.getMetrics.count(\"invalid\", 1); return 'foobar'; }";
    Transform transform = new ScriptFilterTransform(config);
    MockMetrics metrics = new MockMetrics();
    TransformContext transformContext = new MockTransformContext(ImmutableMap.<String, String>of(), metrics);
    transform.initialize(transformContext);

    Schema schema = Schema.recordOf("number", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    StructuredRecord input = StructuredRecord.builder(schema).set("x", 1).build();
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(input, emitter);
    Assert.assertEquals(1, metrics.getCount("invalid"));
  }

  @Test
  public void testSimple() throws Exception {
    ScriptFilterTransform.ScriptFilterConfig config = new ScriptFilterTransform.ScriptFilterConfig();
    config.script = "function shouldFilter(inputRecord) { return inputRecord.x * 1024 < 2048; }";
    Schema schema = Schema.recordOf("number", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    StructuredRecord input = StructuredRecord.builder(schema).set("x", 1).build();
    Transform transform = new ScriptFilterTransform(config);
    TransformContext transformContext = new MockTransformContext(ImmutableMap.<String, String>of());
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(input, emitter);
    Assert.assertTrue(emitter.getEmitted().isEmpty());
    emitter.clear();

    input = StructuredRecord.builder(schema).set("x", 2).build();
    transform.transform(input, emitter);
    Assert.assertEquals(input, emitter.getEmitted().iterator().next());
  }

  @Test
  public void testComplex() throws Exception {
    Schema inner2Schema = Schema.recordOf(
      "inner2",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("val", Schema.of(Schema.Type.DOUBLE))
    );
    Schema inner1Schema = Schema.recordOf(
      "inner1",
      Schema.Field.of("list", Schema.arrayOf(Schema.recordOf(
        "component",
        Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), inner2Schema))
      )))
    );
    Schema schema = Schema.recordOf(
      "complex",
      Schema.Field.of("num", Schema.of(Schema.Type.INT)),
      Schema.Field.of("inner1", inner1Schema)
    );

    /*
    {
      "complex": {
        "num": 8,
        "inner1": {
          "list": [
            "map": {
              "p": {
                "name": "pi",
                "val": 3.14
              },
              "e": {
                "name": "e",
                "val": 2.71
              }
            }
          ]
        }
      }
    }
    */
    StructuredRecord pi = StructuredRecord.builder(inner2Schema).set("name", "pi").set("val", 3.14).build();
    StructuredRecord e = StructuredRecord.builder(inner2Schema).set("name", "e").set("val", 2.71).build();
    StructuredRecord inner1 = StructuredRecord.builder(inner1Schema)
      .set("list", Lists.newArrayList(ImmutableMap.of("p", pi, "e", e)))
      .build();
    StructuredRecord input = StructuredRecord.builder(schema)
      .set("num", 8)
      .set("inner1", inner1)
      .build();

    ScriptFilterTransform.ScriptFilterConfig config = new ScriptFilterTransform.ScriptFilterConfig();
    config.script = "function shouldFilter(rec) {\n" +
      "var pi = rec.inner1.list[0].p;\n" +
      "var e = rec.inner1.list[0].e;\n" +
      "return pi.val > e.val;\n" +
      "}";
    Transform transform = new ScriptFilterTransform(config);
    TransformContext transformContext = new MockTransformContext(ImmutableMap.<String, String>of());
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(input, emitter);
    Assert.assertTrue(emitter.getEmitted().isEmpty());
    emitter.clear();

    config.script = "function shouldFilter(input) {\n" +
      "var pi = input.inner1.list[0].p;\n" +
      "var e = input.inner1.list[0].e;\n" +
      "return pi.val > 10 * e.val;\n" +
      "}";
    transform = new ScriptFilterTransform(config);
    transformContext = new MockTransformContext(ImmutableMap.<String, String>of());
    transform.initialize(transformContext);
    transform.transform(input, emitter);
    Assert.assertEquals(input, emitter.getEmitted().iterator().next());
  }
}
