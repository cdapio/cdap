/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.lineage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldWriteOperation;
import io.cdap.cdap.etl.proto.Connection;
import io.cdap.cdap.etl.proto.v2.spec.PipelineSpec;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FieldLineageProcessorTest {
  private static final PluginSpec DUMMY_PLUGIN =
    new PluginSpec("dummy", "src", ImmutableMap.of(),
                   new ArtifactId("dummy", new ArtifactVersion("1.0.0"), ArtifactScope.USER));

  @Test
  public void testGeneratedOperations() throws Exception {
    // src -> transform1 -> transform2 -> sink
    Schema srcSchema = Schema.recordOf("srcSchema",
                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
                                       Schema.Field.of("offset", Schema.of(Schema.Type.INT)));
    Schema transform1Schema = Schema.recordOf("trans1Schema",
                                              Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    Schema transform2Schema = Schema.recordOf("trans2Schema",
                                              Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                              Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    Set<StageSpec> stageSpecs = ImmutableSet.of(
      StageSpec.builder("src", DUMMY_PLUGIN)
        .addOutput(srcSchema, "transform1")
        .build(),
      StageSpec.builder("transform1", DUMMY_PLUGIN)
        .addInputSchema("src", srcSchema)
        .addOutput(transform1Schema, "transform2")
        .build(),
      StageSpec.builder("transform2", DUMMY_PLUGIN)
        .addInputSchema("transform1", transform1Schema)
        .addOutput(transform2Schema, "sink")
        .build(),
      StageSpec.builder("sink", DUMMY_PLUGIN)
        .addInputSchema("transform2", transform2Schema)
        .build()
    );

    Set<Connection> connections = ImmutableSet.of(
      new Connection("src", "transform1"),
      new Connection("transform1", "transform2"),
      new Connection("transform2", "sink"));

    PipelineSpec pipelineSpec = PipelineSpec.builder().addStages(stageSpecs).addConnections(connections).build();

    FieldLineageProcessor processor = new FieldLineageProcessor(pipelineSpec);

    Map<String, List<FieldOperation>> fieldOperations =
      ImmutableMap.of("src", Collections.singletonList(
        new FieldReadOperation("Read", "1st operation", EndPoint.of("file"), ImmutableList.of("body", "offset"))),
                      "transform1", Collections.emptyList(),
                      "transform2", Collections.emptyList(),
                      "sink", Collections.singletonList(
          new FieldWriteOperation("Write", "4th operation", EndPoint.of("sink"), ImmutableList.of("id", "name"))));
    Set<Operation> operations = processor.validateAndConvert(fieldOperations);

    Set<Operation> expected = ImmutableSet.of(
      new ReadOperation("src.Read", "1st operation", EndPoint.of("file"), ImmutableList.of("body", "offset")),
      new TransformOperation("transform1.Transform", "",
                             ImmutableList.of(InputField.of("src.Read", "body"),
                                              InputField.of("src.Read", "offset")), "body"),
      new TransformOperation("transform2.Transform", "",
                             ImmutableList.of(InputField.of("transform1.Transform", "body")),
                             ImmutableList.of("id", "name")),
      new WriteOperation("sink.Write", "4th operation", EndPoint.of("sink"),
                         ImmutableList.of(InputField.of("transform2.Transform", "id"),
                                          InputField.of("transform2.Transform", "name"))));
    Assert.assertEquals(expected, operations);
  }
}
