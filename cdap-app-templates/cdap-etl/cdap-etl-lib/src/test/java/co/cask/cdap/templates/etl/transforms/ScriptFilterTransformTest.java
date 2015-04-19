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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.templates.etl.api.TransformContext;
import co.cask.cdap.templates.etl.api.TransformStage;
import co.cask.cdap.templates.etl.common.MockEmitter;
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
    TransformStage transform = new ScriptFilterTransform();
    TransformContext transformContext = new MockTransformContext(
      ImmutableMap.of("script", "funtion() { return false; }"));
    transform.initialize(transformContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFilter() throws Exception {
    TransformStage transform = new ScriptFilterTransform();
    TransformContext transformContext = new MockTransformContext(
      ImmutableMap.of("script", "return 'foobar'"));
    transform.initialize(transformContext);

    Schema schema = Schema.recordOf("number", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    StructuredRecord input = StructuredRecord.builder(schema).set("x", 1).build();
    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(input, emitter);
  }

  @Test
  public void testSimple() throws Exception {
    Schema schema = Schema.recordOf("number", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    StructuredRecord input = StructuredRecord.builder(schema).set("x", 1).build();
    TransformStage transform = new ScriptFilterTransform();
    TransformContext transformContext = new MockTransformContext(
      ImmutableMap.of("script", "return input.x * 1024 < 2048"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
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

    TransformStage transform = new ScriptFilterTransform();
    TransformContext transformContext = new MockTransformContext(ImmutableMap.of(
      "script", "var pi = input.inner1.list[0].p; var e = input.inner1.list[0].e; return pi.val > e.val;"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(input, emitter);
    Assert.assertTrue(emitter.getEmitted().isEmpty());
    emitter.clear();

    transform = new ScriptFilterTransform();
    transformContext = new MockTransformContext(ImmutableMap.of(
      "script", "var pi = input.inner1.list[0].p; var e = input.inner1.list[0].e; return pi.val > 10 * e.val;"));
    transform.initialize(transformContext);
    transform.transform(input, emitter);
    Assert.assertEquals(input, emitter.getEmitted().iterator().next());
  }
}
