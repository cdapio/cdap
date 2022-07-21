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

package io.cdap.cdap.etl.batch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests description of BatchPhaseSpec.
 */
public class BatchPhaseSpecTest {

  @Test
  public void testDescription() throws Exception {
    /*
     * source1 --|
     *           |--> sink.connector
     * source2 --|
     */
    Map<String, String> props = new HashMap<>();
    PluginSpec connectorSpec = new PluginSpec(Constants.Connector.PLUGIN_TYPE, "connector",
                                              ImmutableMap.<String, String>of(), null);
    ArtifactId artifactId = new ArtifactId("art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    PipelinePhase.Builder builder =
      PipelinePhase.builder(ImmutableSet.of(BatchSource.PLUGIN_TYPE, Constants.Connector.PLUGIN_TYPE))
        .addStage(StageSpec.builder("source1", new PluginSpec(BatchSource.PLUGIN_TYPE, "src", props, artifactId))
                    .build())
        .addStage(StageSpec.builder("source2", new PluginSpec(BatchSource.PLUGIN_TYPE, "src", props, artifactId))
                    .addInputSchema("a", Schema.recordOf("stuff", Schema.Field.of("x", Schema.of(Schema.Type.INT))))
                    .build())
        .addStage(StageSpec.builder("sink.connector", connectorSpec).build())
        .addConnection("source1", "sink.connector")
        .addConnection("source2", "sink.connector");

    BatchPhaseSpec phaseSpec =
      new BatchPhaseSpec("phase-1", builder.build(), new Resources(), new Resources(), new Resources(),
                         false, false, Collections.<String, String>emptyMap(),
                         0, Collections.<String, String>emptyMap(), false, null);
    Assert.assertEquals("Sources 'source1', 'source2' to sinks 'sink.connector'.", phaseSpec.getDescription());
  }
}
