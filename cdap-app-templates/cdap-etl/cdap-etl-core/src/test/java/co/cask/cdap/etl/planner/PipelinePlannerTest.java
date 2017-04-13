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

package co.cask.cdap.etl.planner;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.spec.PipelineSpec;
import co.cask.cdap.etl.spec.PluginSpec;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class PipelinePlannerTest {
  private static final String NODE = "node";
  private static final String AGGREGATOR = "aggregator";

  @Test
  public void testGeneratePlan() {
    /*
             |--- n2(r) ----------|
             |                    |                                    |-- n10
        n1 --|--- n3(r) --- n5 ---|--- n6 --- n7(r) --- n8 --- n9(r) --|
             |                    |                                    |-- n11
             |--- n4(r) ----------|
     */
    // create the spec for this pipeline
    ArtifactId artifactId = new ArtifactId("dummy", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM);
    Map<String, String> empty = ImmutableMap.of();
    PluginSpec nodePlugin = new PluginSpec(NODE, "mock", empty, artifactId);
    PluginSpec reducePlugin = new PluginSpec(AGGREGATOR, "mock", empty, artifactId);
    Schema schema = Schema.recordOf("stuff", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Set<StageSpec> stageSpecs = ImmutableSet.of(
      StageSpec.builder("n1", nodePlugin)
        .setOutputSchema(schema)
        .addOutputs("n2", "n3", "n4")
        .build(),
      StageSpec.builder("n2", reducePlugin)
        .addInputSchema("n1", schema)
        .setOutputSchema(schema)
        .addInputs("n1")
        .addOutputs("n6")
        .build(),
      StageSpec.builder("n3", reducePlugin)
        .addInputSchema("n1", schema)
        .setOutputSchema(schema)
        .addInputs("n1")
        .addOutputs("n5")
        .build(),
      StageSpec.builder("n4", reducePlugin)
        .addInputSchema("n1", schema)
        .setOutputSchema(schema)
        .addInputs("n1")
        .addOutputs("n6")
        .build(),
      StageSpec.builder("n5", nodePlugin)
        .addInputSchema("n3", schema)
        .setOutputSchema(schema)
        .addInputs("n3")
        .addOutputs("n6")
        .build(),
      StageSpec.builder("n6", nodePlugin)
        .addInputSchemas(ImmutableMap.of("n2", schema, "n5", schema, "n4", schema))
        .setOutputSchema(schema)
        .addInputs("n2", "n5", "n4")
        .addOutputs("n7")
        .build(),
      StageSpec.builder("n7", reducePlugin)
        .addInputSchema("n6", schema)
        .setOutputSchema(schema)
        .addInputs("n6")
        .addOutputs("n8")
        .build(),
      StageSpec.builder("n8", nodePlugin)
        .addInputSchema("n7", schema)
        .setOutputSchema(schema)
        .addInputs("n7")
        .addOutputs("n9")
        .build(),
      StageSpec.builder("n9", reducePlugin)
        .addInputSchema("n8", schema)
        .setOutputSchema(schema)
        .addInputs("n8")
        .addOutputs("n10", "n11")
        .build(),
      StageSpec.builder("n10", nodePlugin)
        .addInputSchema("n9", schema)
        .addInputs("n9")
        .build(),
      StageSpec.builder("n11", nodePlugin)
        .addInputSchema("n9", schema)
        .addInputs("n9")
        .build()
    );
    Set<Connection> connections = ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3"),
      new Connection("n1", "n4"),
      new Connection("n2", "n6"),
      new Connection("n3", "n5"),
      new Connection("n4", "n6"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7"),
      new Connection("n7", "n8"),
      new Connection("n8", "n9"),
      new Connection("n9", "n10"),
      new Connection("n9", "n11")
    );
    Set<String> pluginTypes = ImmutableSet.of(NODE, AGGREGATOR, Constants.CONNECTOR_TYPE);
    Set<String> reduceTypes = ImmutableSet.of(AGGREGATOR);
    Set<String> emptySet = ImmutableSet.of();
    PipelinePlanner planner = new PipelinePlanner(pluginTypes, reduceTypes, emptySet, emptySet);
    PipelineSpec pipelineSpec = PipelineSpec.builder().addStages(stageSpecs).addConnections(connections).build();

    Map<String, PipelinePhase> phases = new HashMap<>();
    /*
             |--- n2.connector
             |
        n1 --|--- n3.connector
             |
             |--- n4.connector
     */
    PipelinePhase phase1 = PipelinePhase.builder(pluginTypes)
      .addStage(StageInfo.builder("n1", NODE).addOutputs("n2", "n3", "n4").setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n2.connector", Constants.CONNECTOR_TYPE).build())
      .addStage(StageInfo.builder("n3.connector", Constants.CONNECTOR_TYPE).build())
      .addStage(StageInfo.builder("n4.connector", Constants.CONNECTOR_TYPE).build())
      .addConnections("n1", ImmutableSet.of("n2.connector", "n3.connector", "n4.connector"))
      .build();
    String phase1Name = getPhaseName("n1", "n2.connector", "n3.connector", "n4.connector");
    phases.put(phase1Name, phase1);

    /*
        phase2:
        n2.connector --- n2(r) --- n6 --- n7.connector
     */
    PipelinePhase phase2 = PipelinePhase.builder(pluginTypes)
      .addStage(StageInfo.builder("n2", AGGREGATOR)
                  .addInputs("n1")
                  .addInputSchema("n1", schema)
                  .addOutputs("n6")
                  .setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n6", NODE)
                  .addInputs("n2", "n4", "n5")
                  .addInputSchema("n2", schema)
                  .addInputSchema("n4", schema)
                  .addInputSchema("n5", schema)
                  .addOutputs("n7")
                  .setOutputSchema(schema)
                  .build())
      .addStage(StageInfo.builder("n2.connector", Constants.CONNECTOR_TYPE).build())
      .addStage(StageInfo.builder("n7.connector", Constants.CONNECTOR_TYPE).build())
      .addConnection("n2.connector", "n2")
      .addConnection("n2", "n6")
      .addConnection("n6", "n7.connector")
      .build();
    String phase2Name = getPhaseName("n2.connector", "n7.connector");
    phases.put(phase2Name, phase2);

    /*
        phase3:
        n3.connector --- n3(r) --- n5 --- n6 --- n7.connector
     */
    PipelinePhase phase3 = PipelinePhase.builder(pluginTypes)
      .addStage(StageInfo.builder("n5", NODE)
                  .addInputs("n3")
                  .addInputSchema("n3", schema)
                  .addOutputs("n6")
                  .setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n6", NODE)
                  .addInputs("n2", "n4", "n5")
                  .addInputSchema("n2", schema)
                  .addInputSchema("n4", schema)
                  .addInputSchema("n5", schema)
                  .addOutputs("n7")
                  .setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n3", AGGREGATOR)
                  .addInputs("n1")
                  .addInputSchema("n1", schema)
                  .addOutputs("n5")
                  .setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n3.connector", Constants.CONNECTOR_TYPE).build())
      .addStage(StageInfo.builder("n7.connector", Constants.CONNECTOR_TYPE).build())
      .addConnection("n3.connector", "n3")
      .addConnection("n3", "n5")
      .addConnection("n5", "n6")
      .addConnection("n6", "n7.connector")
      .build();
    String phase3Name = getPhaseName("n3.connector", "n7.connector");
    phases.put(phase3Name, phase3);

    /*
        phase4:
        n4.connector --- n4(r) --- n6 --- n7.connector
     */
    PipelinePhase phase4 = PipelinePhase.builder(pluginTypes)
      .addStage(StageInfo.builder("n4", AGGREGATOR)
                  .addInputs("n1")
                  .addInputSchema("n1", schema)
                  .addOutputs("n6")
                  .setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n6", NODE)
                  .addInputs("n2", "n4", "n5")
                  .addInputSchema("n2", schema)
                  .addInputSchema("n4", schema)
                  .addInputSchema("n5", schema)
                  .addOutputs("n7")
                  .setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n4.connector", Constants.CONNECTOR_TYPE).build())
      .addStage(StageInfo.builder("n7.connector", Constants.CONNECTOR_TYPE).build())
      .addConnection("n4.connector", "n4")
      .addConnection("n4", "n6")
      .addConnection("n6", "n7.connector")
      .build();
    String phase4Name = getPhaseName("n4.connector", "n7.connector");
    phases.put(phase4Name, phase4);

    /*
        phase5:
        n7.connector --- n7(r) --- n8 --- n9.connector
     */
    PipelinePhase phase5 = PipelinePhase.builder(pluginTypes)
      .addStage(StageInfo.builder("n8", NODE)
                  .addInputs("n7")
                  .addInputSchema("n7", schema)
                  .addOutputs("n9")
                  .setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n7", AGGREGATOR)
                  .addInputs("n6")
                  .addInputSchema("n6", schema)
                  .addOutputs("n8")
                  .setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n7.connector", Constants.CONNECTOR_TYPE).build())
      .addStage(StageInfo.builder("n9.connector", Constants.CONNECTOR_TYPE).build())
      .addConnection("n7.connector", "n7")
      .addConnection("n7", "n8")
      .addConnection("n8", "n9.connector")
      .build();
    String phase5Name = getPhaseName("n7.connector", "n9.connector");
    phases.put(phase5Name, phase5);

    /*
        phase6:
                                 |-- n10
        n9.connector --- n9(r) --|
                                 |-- n11
     */
    PipelinePhase phase6 = PipelinePhase.builder(pluginTypes)
      .addStage(StageInfo.builder("n10", NODE)
                  .addInputs("n9")
                  .addInputSchema("n9", schema).build())
      .addStage(StageInfo.builder("n11", NODE)
                  .addInputs("n9")
                  .addInputSchema("n9", schema).build())
      .addStage(StageInfo.builder("n9", AGGREGATOR)
                  .addInputs("n8")
                  .addInputSchema("n8", schema)
                  .addOutputs("n10", "n11")
                  .setOutputSchema(schema).build())
      .addStage(StageInfo.builder("n9.connector", Constants.CONNECTOR_TYPE).build())
      .addConnection("n9.connector", "n9")
      .addConnection("n9", "n10")
      .addConnection("n9", "n11")
      .build();
    String phase6Name = getPhaseName("n9.connector", "n10", "n11");
    phases.put(phase6Name, phase6);

    Set<Connection> phaseConnections = new HashSet<>();
    phaseConnections.add(new Connection(phase1Name, phase2Name));
    phaseConnections.add(new Connection(phase1Name, phase3Name));
    phaseConnections.add(new Connection(phase1Name, phase4Name));
    phaseConnections.add(new Connection(phase2Name, phase5Name));
    phaseConnections.add(new Connection(phase3Name, phase5Name));
    phaseConnections.add(new Connection(phase4Name, phase5Name));
    phaseConnections.add(new Connection(phase5Name, phase6Name));

    PipelinePlan expected = new PipelinePlan(phases, phaseConnections);
    PipelinePlan actual = planner.plan(pipelineSpec);
    Assert.assertEquals(expected, actual);
  }

  private static String getPhaseName(String source, String... sinks) {
    Set<String> sources = ImmutableSet.of(source);
    Set<String> sinkNames = new HashSet<>();
    Collections.addAll(sinkNames, sinks);
    return PipelinePlanner.getPhaseName(sources, sinkNames);
  }
}
