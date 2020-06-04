/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for SparkPipelineRunner
 */
public class SparkPipelineRunnerTest {

  @Test
  public void testDeriveKeySchema() {
    /*
        A: x(int), y(string)
        B: x(int), yy(string), z(long)

        select A.x as A_x, A.y, B.x as B_x, B.z
        from A join B on A.x = B.x and A.y = B.yy
     */
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("A_x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("B_x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("z", Schema.of(Schema.Type.LONG)));

    JoinStage stageA = JoinStage.builder("A", null).build();
    JoinStage stageB = JoinStage.builder("B", null).build();

    JoinDefinition joinDefinition = JoinDefinition.builder()
      .select(new JoinField("A", "x", "A_x"), new JoinField("A", "y"),
              new JoinField("B", "x", "B_x"), new JoinField("B", "z"))
      .from(stageA, stageB)
      .on(JoinCondition.onKeys()
            .addKey(new JoinKey("A", Arrays.asList("x", "y")))
            .addKey(new JoinKey("B", Arrays.asList("x", "yy"))).build())
      .setOutputSchema(outputSchema)
      .build();

    Map<String, List<String>> keys = new HashMap<>();
    keys.put("A", Arrays.asList("x", "y"));
    keys.put("B", Arrays.asList("x", "yy"));

    List<Schema> keySchema = SparkPipelineRunner.deriveKeySchema("joiner", keys, joinDefinition);
    List<Schema> expected = Arrays.asList(
      Schema.nullableOf(Schema.of(Schema.Type.INT)),
      Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expected, keySchema);
  }

  @Test
  public void testDeriveInputSchema() {
    /*
        A: x(int), y(string)
        B: x(int), yy(string), z(long)

        select A.x as A_x, A.y, B.x as B_x, B.z as zzz
        from A join B on A.x = B.x and A.y = B.yy
     */
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("A_x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("B_x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("zzz", Schema.of(Schema.Type.LONG)));

    List<JoinField> selectedFields = Arrays.asList(new JoinField("A", "x", "A_x"), new JoinField("A", "y"),
                                                   new JoinField("B", "x", "B_x"), new JoinField("B", "z", "zzz"));

    List<Schema> keySchema = Arrays.asList(
      Schema.nullableOf(Schema.of(Schema.Type.INT)),
      Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    Schema derived = SparkPipelineRunner.deriveInputSchema("joiner", "A", Arrays.asList("x", "y"), keySchema,
                                                           selectedFields, outputSchema);
    Schema expected = Schema.recordOf(
      "A",
      Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("y", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Assert.assertEquals(expected, derived);

    derived = SparkPipelineRunner.deriveInputSchema("joiner", "B", Arrays.asList("x", "yy"), keySchema,
                                                    selectedFields, outputSchema);
    expected = Schema.recordOf(
      "B",
      Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("yy", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("z", Schema.of(Schema.Type.LONG)));
    Assert.assertEquals(expected, derived);
  }
}
