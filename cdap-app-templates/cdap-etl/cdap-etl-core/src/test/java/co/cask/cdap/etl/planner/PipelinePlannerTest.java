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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class PipelinePlannerTest {
  private static final PluginSpec NODE =
    new PluginSpec("node", "name", ImmutableMap.<String, String>of(),
                   new ArtifactId("dummy", new ArtifactVersion("1.0.0"), ArtifactScope.USER));
  private static final PluginSpec REDUCE =
    new PluginSpec("reduce", "name", ImmutableMap.<String, String>of(),
                   new ArtifactId("dummy", new ArtifactVersion("1.0.0"), ArtifactScope.USER));
  private static final PluginSpec ACTION =
    new PluginSpec("action", "action", ImmutableMap.<String, String>of(), null);
  private static final PluginSpec CONDITION =
    new PluginSpec("condition", "condition", ImmutableMap.<String, String>of(), null);
  private static final PluginSpec CONDITION1 =
    new PluginSpec("condition", "condition1", ImmutableMap.<String, String>of(), null);
  private static final PluginSpec CONDITION2 =
    new PluginSpec("condition", "condition2", ImmutableMap.<String, String>of(), null);
  private static final PluginSpec CONDITION3 =
    new PluginSpec("condition", "condition3", ImmutableMap.<String, String>of(), null);
  private static final PluginSpec CONDITION4 =
    new PluginSpec("condition", "condition4", ImmutableMap.<String, String>of(), null);
  private static final PluginSpec CONDITION5 =
    new PluginSpec("condition", "condition5", ImmutableMap.<String, String>of(), null);

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
    Schema schema = Schema.recordOf("stuff", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Set<StageSpec> stageSpecs = ImmutableSet.of(
      StageSpec.builder("n1", NODE)
        .addOutputSchema(schema, "n2", "n3", "n4")
        .build(),
      StageSpec.builder("n2", REDUCE)
        .addInputSchema("n1", schema)
        .addOutputSchema(schema, "n6")
        .build(),
      StageSpec.builder("n3", REDUCE)
        .addInputSchema("n1", schema)
        .addOutputSchema(schema, "n5")
        .build(),
      StageSpec.builder("n4", REDUCE)
        .addInputSchema("n1", schema)
        .addOutputSchema(schema, "n6")
        .build(),
      StageSpec.builder("n5", NODE)
        .addInputSchema("n3", schema)
        .addOutputSchema(schema, "n6")
        .build(),
      StageSpec.builder("n6", NODE)
        .addInputSchemas(ImmutableMap.of("n2", schema, "n5", schema, "n4", schema))
        .addOutputSchema(schema, "n7")
        .build(),
      StageSpec.builder("n7", REDUCE)
        .addInputSchema("n6", schema)
        .addOutputSchema(schema, "n8")
        .build(),
      StageSpec.builder("n8", NODE)
        .addInputSchema("n7", schema)
        .addOutputSchema(schema, "n9")
        .build(),
      StageSpec.builder("n9", REDUCE)
        .addInputSchema("n8", schema)
        .addOutputSchema(schema, "n10", "n11")
        .build(),
      StageSpec.builder("n10", NODE)
        .addInputSchema("n9", schema)
        .build(),
      StageSpec.builder("n11", NODE)
        .addInputSchema("n9", schema)
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
    Set<String> pluginTypes = ImmutableSet.of(NODE.getType(), REDUCE.getType(), Constants.Connector.PLUGIN_TYPE);
    Set<String> reduceTypes = ImmutableSet.of(REDUCE.getType());
    Set<String> emptySet = ImmutableSet.of();
    PipelinePlanner planner = new PipelinePlanner(pluginTypes, reduceTypes, emptySet, emptySet, emptySet);
    PipelineSpec pipelineSpec = PipelineSpec.builder().addStages(stageSpecs).addConnections(connections).build();

    Map<String, PipelinePhase> phases = new HashMap<>();
    /*
        n1 --> n1.out.connector
     */
    PipelinePhase phase1 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n1", NODE).addOutputSchema(schema, "n2", "n3", "n4").build())
      .addStage(StageSpec.builder("n1.out.connector",
                                  connectorSpec("n1.out.connector", Constants.Connector.SINK_TYPE)).build())
      .addConnections("n1", ImmutableSet.of("n1.out.connector"))
      .build();
    String phase1Name = PipelinePlanner.getPhaseName(phase1.getDag());
    phases.put(phase1Name, phase1);

    /*
        phase2:
        n1.out.connector --- n2(r) --- n6 --- n7.connector
     */
    PipelinePhase phase2 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n2", REDUCE)
                  .addInputSchema("n1", schema)
                  .addOutputSchema(schema, "n6")
                  .build())
      .addStage(StageSpec.builder("n6", NODE)
                  .addInputSchema("n2", schema)
                  .addInputSchema("n4", schema)
                  .addInputSchema("n5", schema)
                  .addOutputSchema(schema, "n7")
                  .build())
      .addStage(StageSpec.builder("n1.out.connector",
                                  connectorSpec("n1.out.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n7.connector", connectorSpec("n7", Constants.Connector.SINK_TYPE)).build())
      .addConnection("n1.out.connector", "n2")
      .addConnection("n2", "n6")
      .addConnection("n6", "n7.connector")
      .build();
    String phase2Name = PipelinePlanner.getPhaseName(phase2.getDag());
    phases.put(phase2Name, phase2);

    /*
        phase3:
        n1.out.connector --- n3(r) --- n5 --- n6 --- n7.connector
     */
    PipelinePhase phase3 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n5", NODE)
                  .addInputSchema("n3", schema)
                  .addOutputSchema(schema, "n6")
                  .build())
      .addStage(StageSpec.builder("n6", NODE)
                  .addInputSchema("n2", schema)
                  .addInputSchema("n4", schema)
                  .addInputSchema("n5", schema)
                  .addOutputSchema(schema, "n7")
                  .build())
      .addStage(StageSpec.builder("n3", REDUCE)
                  .addInputSchema("n1", schema)
                  .addOutputSchema(schema, "n5")
                  .build())
      .addStage(StageSpec.builder("n1.out.connector",
                                  connectorSpec("n1.out.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n7.connector", connectorSpec("n7", Constants.Connector.SINK_TYPE)).build())
      .addConnection("n1.out.connector", "n3")
      .addConnection("n3", "n5")
      .addConnection("n5", "n6")
      .addConnection("n6", "n7.connector")
      .build();
    String phase3Name = PipelinePlanner.getPhaseName(phase3.getDag());
    phases.put(phase3Name, phase3);

    /*
        phase4:
        n1.out.connector --- n4(r) --- n6 --- n7.connector
     */
    PipelinePhase phase4 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n4", REDUCE)
                  .addInputSchema("n1", schema)
                  .addOutputSchema(schema, "n6")
                  .build())
      .addStage(StageSpec.builder("n6", NODE)
                  .addInputSchema("n2", schema)
                  .addInputSchema("n4", schema)
                  .addInputSchema("n5", schema)
                  .addOutputSchema(schema, "n7")
                  .build())
      .addStage(StageSpec.builder("n1.out.connector",
                                  connectorSpec("n1.out.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n7.connector", connectorSpec("n7", Constants.Connector.SINK_TYPE)).build())
      .addConnection("n1.out.connector", "n4")
      .addConnection("n4", "n6")
      .addConnection("n6", "n7.connector")
      .build();
    String phase4Name = PipelinePlanner.getPhaseName(phase4.getDag());
    phases.put(phase4Name, phase4);

    /*
        phase5:
        n7.connector --- n7(r) --- n8 --- n9.connector
     */
    PipelinePhase phase5 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n8", NODE)
                  .addInputSchema("n7", schema)
                  .addOutputSchema(schema, "n9")
                  .build())
      .addStage(StageSpec.builder("n7", REDUCE)
                  .addInputSchema("n6", schema)
                  .addOutputSchema(schema, "n8")
                  .build())
      .addStage(StageSpec.builder("n7.connector", connectorSpec("n7", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n9.connector", connectorSpec("n9", Constants.Connector.SINK_TYPE)).build())
      .addConnection("n7.connector", "n7")
      .addConnection("n7", "n8")
      .addConnection("n8", "n9.connector")
      .build();
    String phase5Name = PipelinePlanner.getPhaseName(phase5.getDag());
    phases.put(phase5Name, phase5);

    /*
        phase6:
                                 |-- n10
        n9.connector --- n9(r) --|
                                 |-- n11
     */
    PipelinePhase phase6 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n10", NODE).addInputSchema("n9", schema).build())
      .addStage(StageSpec.builder("n11", NODE).addInputSchema("n9", schema).build())
      .addStage(StageSpec.builder("n9", REDUCE)
                  .addInputSchema("n8", schema)
                  .addOutputSchema(schema, "n10", "n11")
                  .build())
      .addStage(StageSpec.builder("n9.connector", connectorSpec("n9", Constants.Connector.SOURCE_TYPE)).build())
      .addConnection("n9.connector", "n9")
      .addConnection("n9", "n10")
      .addConnection("n9", "n11")
      .build();
    String phase6Name = PipelinePlanner.getPhaseName(phase6.getDag());
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

  @Test
  public void testSimpleCondition() throws Exception {
    /*
      n1 - n2 - condition - n3
                      |
                      |---- n4
     */

    Set<StageSpec> stageSpecs = ImmutableSet.of(
      StageSpec.builder("n1", NODE)
        .build(),
      StageSpec.builder("n2", NODE)
        .build(),
      StageSpec.builder("condition", CONDITION)
        .build(),
      StageSpec.builder("n3", NODE)
        .build(),
      StageSpec.builder("n4", NODE)
        .build()
    );

    Set<Connection> connections = ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "condition"),
      new Connection("condition", "n3", true),
      new Connection("condition", "n4", false)
    );

    Set<String> pluginTypes = ImmutableSet.of(NODE.getType(), REDUCE.getType(), Constants.Connector.PLUGIN_TYPE,
                                              CONDITION.getType());
    Set<String> reduceTypes = ImmutableSet.of(REDUCE.getType());
    Set<String> emptySet = ImmutableSet.of();
    PipelinePlanner planner = new PipelinePlanner(pluginTypes, reduceTypes, emptySet, emptySet, emptySet);
    PipelineSpec pipelineSpec = PipelineSpec.builder().addStages(stageSpecs).addConnections(connections).build();


    Map<String, PipelinePhase> phases = new HashMap<>();
    /*
      n1--n2--condition.connector
     */
    PipelinePhase phase1 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n1", NODE).build())
      .addStage(StageSpec.builder("n2", NODE).build())
      .addStage(StageSpec.builder("condition.connector",
                                  connectorSpec("condition.connector", Constants.Connector.SINK_TYPE)).build())
      .addConnection("n1", "n2")
      .addConnection("n2", "condition.connector")
      .build();
    Dag controlPhaseDag = new Dag(ImmutableSet.of(new Connection("n1", "n2"), new Connection("n2", "condition")));
    String phase1Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase1Name, phase1);

    /*
      condition
     */
    PipelinePhase phase2 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition", CONDITION).build())
      .build();
    String phase2Name = "condition";
    phases.put(phase2Name, phase2);

    /*
      condition.connector -- n3
     */
    PipelinePhase phase3 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition.connector",
                                  connectorSpec("condition.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n3", NODE).build())
      //.addConnection("condition.connector", "n3", true)
      .addConnection("condition.connector", "n3")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition", "n3")));
    String phase3Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase3Name, phase3);

     /*
      condition.connector -- n4
     */
    PipelinePhase phase4 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition.connector",
                                  connectorSpec("condition.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n4", NODE).build())
      .addConnection("condition.connector", "n4")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition", "n4")));
    String phase4Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase4Name, phase4);

    Set<Connection> phaseConnections = new HashSet<>();
    phaseConnections.add(new Connection(phase1Name, phase2Name));
    phaseConnections.add(new Connection(phase2Name, phase3Name, true));
    phaseConnections.add(new Connection(phase2Name, phase4Name, false));

    PipelinePlan expected = new PipelinePlan(phases, phaseConnections);
    PipelinePlan actual = planner.plan(pipelineSpec);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMultipleConditions() throws Exception {
   /*
      n1 - n2 - condition1 - n3 - n4 - condition2 - n5 - condition3 - n6
                    |                       |                   |
                    |--n10                  |---condition4 - n8 |------n7
                                                  |
                                                  |----condition5 - n9
     */

    Set<StageSpec> stageSpecs = ImmutableSet.of(
      StageSpec.builder("n1", NODE)
        .build(),
      StageSpec.builder("n2", NODE)
        .build(),
      StageSpec.builder("condition1", CONDITION1)
        .build(),
      StageSpec.builder("n3", NODE)
        .build(),
      StageSpec.builder("n4", NODE)
        .build(),
      StageSpec.builder("condition2", CONDITION2)
        .build(),
      StageSpec.builder("n5", NODE)
        .build(),
      StageSpec.builder("condition3", CONDITION3)
        .build(),
      StageSpec.builder("n6", NODE)
        .build(),
      StageSpec.builder("condition4", CONDITION4)
        .build(),
      StageSpec.builder("n7", NODE)
        .build(),
      StageSpec.builder("condition5", CONDITION5)
        .build(),
      StageSpec.builder("n8", NODE)
        .build(),
      StageSpec.builder("n9", NODE)
        .build(),
      StageSpec.builder("n10", NODE)
        .build()
      );

    Set<Connection> connections = ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "condition1"),
      new Connection("condition1", "n3", true),
      new Connection("condition1", "n10", false),
      new Connection("n3", "n4"),
      new Connection("n4", "condition2"),
      new Connection("condition2", "n5", true),
      new Connection("n5", "condition3"),
      new Connection("condition3", "n6", true),
      new Connection("condition3", "n7", false),
      new Connection("condition2", "condition4", false),
      new Connection("condition4", "n8", true),
      new Connection("condition4", "condition5", false),
      new Connection("condition5", "n9", true)
    );

    Set<String> pluginTypes = ImmutableSet.of(NODE.getType(), REDUCE.getType(), Constants.Connector.PLUGIN_TYPE,
                                              CONDITION1.getType(), CONDITION2.getType(), CONDITION3.getType(),
                                              CONDITION4.getType(), CONDITION5.getType());
    Set<String> reduceTypes = ImmutableSet.of(REDUCE.getType());
    Set<String> emptySet = ImmutableSet.of();
    PipelinePlanner planner = new PipelinePlanner(pluginTypes, reduceTypes, emptySet, emptySet, emptySet);
    PipelineSpec pipelineSpec = PipelineSpec.builder().addStages(stageSpecs).addConnections(connections).build();

    Map<String, PipelinePhase> phases = new HashMap<>();
    /*
      n1--n2--condition1.connector
     */
    PipelinePhase phase1 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n1", NODE).build())
      .addStage(StageSpec.builder("n2", NODE).build())
      .addStage(StageSpec.builder("condition1.connector",
                                  connectorSpec("condition1.connector", Constants.Connector.SINK_TYPE)).build())
      .addConnection("n1", "n2")
      .addConnection("n2", "condition1.connector")
      .build();
    Dag controlPhaseDag = new Dag(ImmutableSet.of(new Connection("n1", "n2"), new Connection("n2", "condition1")));
    String phase1Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase1Name, phase1);

    /*
      condition1
     */
    PipelinePhase phase2 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition1", CONDITION1).build())
      .build();
    String phase2Name = "condition1";
    phases.put(phase2Name, phase2);

    /*
      condition1.connector -- n3 - n4 - condition2.connector
     */
    PipelinePhase phase3 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition1.connector",
                                  connectorSpec("condition1.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("condition2.connector",
                                  connectorSpec("condition2.connector", Constants.Connector.SINK_TYPE)).build())
      .addStage(StageSpec.builder("n3", NODE).build())
      .addStage(StageSpec.builder("n4", NODE).build())
      .addConnection("condition1.connector", "n3")
      .addConnection("n3", "n4")
      .addConnection("n4", "condition2.connector")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(
      new Connection("condition1", "n3"), new Connection("n3", "n4"), new Connection("n4", "condition2")));
    String phase3Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase3Name, phase3);

    /*
      condition1.connector -- n10
     */
    PipelinePhase phase4 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition1.connector",
                                  connectorSpec("condition1.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n10", NODE).build())
      .addConnection("condition1.connector", "n10")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition1", "n10")));
    String phase4Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase4Name, phase4);

    /*
      condition2
     */
    PipelinePhase phase5 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition2", CONDITION2).build())
      .build();
    String phase5Name = "condition2";
    phases.put(phase5Name, phase5);

    /*
      condition2.connector -- n5 -- condition3.connector
     */
    PipelinePhase phase6 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition2.connector",
                                  connectorSpec("condition2.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n5", NODE).build())
      .addStage(StageSpec.builder("condition3.connector",
                                  connectorSpec("condition3.connector", Constants.Connector.SINK_TYPE)).build())
      .addConnection("condition2.connector", "n5")
      .addConnection("n5", "condition3.connector")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition2", "n5"), new Connection("n5", "condition3")));
    String phase6Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase6Name, phase6);

    /*
      condition3
     */
    PipelinePhase phase7 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition3", CONDITION3).build())
      .build();
    String phase7Name = "condition3";
    phases.put(phase7Name, phase7);

    /*
      condition3.connector -- n6
     */
    PipelinePhase phase8 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n6", NODE).build())
      .addStage(StageSpec.builder("condition3.connector",
                                  connectorSpec("condition3.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addConnection("condition3.connector", "n6")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition3", "n6")));
    String phase8Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase8Name, phase8);

    /*
      condition3.connector -- n7
     */
    PipelinePhase phase9 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n7", NODE).build())
      .addStage(StageSpec.builder("condition3.connector",
                                  connectorSpec("condition3.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addConnection("condition3.connector", "n7")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition3", "n7")));
    String phase9Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase9Name, phase9);

    /*
      condition4
     */
    PipelinePhase phase10 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition4", CONDITION4).build())
      .build();
    String phase10Name = "condition4";
    phases.put(phase10Name, phase10);

    /*
      condition4(condition2.connector) -- n8
     */
    PipelinePhase phase11 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n8", NODE).build())
      .addStage(StageSpec.builder("condition2.connector",
                                  connectorSpec("condition2.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addConnection("condition2.connector", "n8")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition4", "n8")));
    String phase11Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase11Name, phase11);

    /*
      condition5
     */
    PipelinePhase phase12 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition5", CONDITION5).build())
      .build();
    String phase12Name = "condition5";
    phases.put(phase12Name, phase12);

    /*
      condition5(condition2.connector) -- n9
     */
    PipelinePhase phase13 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n9", NODE).build())
      .addStage(StageSpec.builder("condition2.connector",
                                  connectorSpec("condition2.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addConnection("condition2.connector", "n9")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition5", "n9")));
    String phase13Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase13Name, phase13);

    Set<Connection> phaseConnections = new HashSet<>();
    phaseConnections.add(new Connection(phase1Name, phase2Name));
    phaseConnections.add(new Connection(phase2Name, phase3Name, true));
    phaseConnections.add(new Connection(phase2Name, phase4Name, false));
    phaseConnections.add(new Connection(phase3Name, phase5Name));
    phaseConnections.add(new Connection(phase5Name, phase6Name, true));
    phaseConnections.add(new Connection(phase6Name, phase7Name));
    phaseConnections.add(new Connection(phase7Name, phase8Name, true));
    phaseConnections.add(new Connection(phase7Name, phase9Name, false));
    phaseConnections.add(new Connection(phase5Name, phase10Name, false));
    phaseConnections.add(new Connection(phase10Name, phase11Name, true));
    phaseConnections.add(new Connection(phase10Name, phase12Name, false));
    phaseConnections.add(new Connection(phase12Name, phase13Name, true));

    PipelinePlan expected = new PipelinePlan(phases, phaseConnections);
    PipelinePlan actual = planner.plan(pipelineSpec);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testConditionsToConditions() throws Exception {
   /*
      n1 - c1----c2---n2
           |
           |-----c3---n3
     */

    Set<StageSpec> stageSpecs = ImmutableSet.of(
      StageSpec.builder("n1", NODE)
        .build(),
      StageSpec.builder("n2", NODE)
        .build(),
      StageSpec.builder("condition1", CONDITION1)
        .build(),
      StageSpec.builder("n3", NODE)
        .build(),
      StageSpec.builder("condition2", CONDITION2)
        .build(),
      StageSpec.builder("condition3", CONDITION3)
        .build()
    );

    Set<Connection> connections = ImmutableSet.of(
      new Connection("n1", "condition1"),
      new Connection("condition1", "condition2", true),
      new Connection("condition1", "condition3", false),
      new Connection("condition2", "n2", true),
      new Connection("condition3", "n3", true)
    );

    Set<String> pluginTypes = ImmutableSet.of(NODE.getType(), REDUCE.getType(), Constants.Connector.PLUGIN_TYPE,
                                              CONDITION1.getType(), CONDITION2.getType(), CONDITION3.getType(),
                                              CONDITION4.getType(), CONDITION5.getType());
    Set<String> reduceTypes = ImmutableSet.of(REDUCE.getType());
    Set<String> emptySet = ImmutableSet.of();
    PipelinePlanner planner = new PipelinePlanner(pluginTypes, reduceTypes, emptySet, emptySet, emptySet);
    PipelineSpec pipelineSpec = PipelineSpec.builder().addStages(stageSpecs).addConnections(connections).build();
    Map<String, PipelinePhase> phases = new HashMap<>();
    /*
      n1--condition1.connector
     */
    PipelinePhase phase1 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n1", NODE).build())
      .addStage(StageSpec.builder("condition1.connector",
                                  connectorSpec("condition1.connector", Constants.Connector.SINK_TYPE)).build())
      .addConnection("n1", "condition1.connector")
      .build();

    Dag controlPhaseDag = new Dag(ImmutableSet.of(new Connection("n1", "condition1")));
    String phase1Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase1Name, phase1);

    /*
      condition1
     */
    PipelinePhase phase2 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition1", CONDITION1).build())
      .build();
    String phase2Name = "condition1";
    phases.put(phase2Name, phase2);

    /*
      condition2
     */
    PipelinePhase phase3 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition2", CONDITION2).build())
      .build();
    String phase3Name = "condition2";
    phases.put(phase3Name, phase3);

    /*
      condition3
     */
    PipelinePhase phase4 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition3", CONDITION3).build())
      .build();
    String phase4Name = "condition3";
    phases.put(phase4Name, phase4);


    /*
      condition1.connector -- n2
     */
    PipelinePhase phase5 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition1.connector",
                                  connectorSpec("condition1.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n2", NODE).build())
      .addConnection("condition1.connector", "n2")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition2", "n2")));
    String phase5Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase5Name, phase5);

    /*
      condition1.connector -- n3
     */
    PipelinePhase phase6 = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("condition1.connector",
                                  connectorSpec("condition1.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n3", NODE).build())
      .addConnection("condition1.connector", "n3")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("condition3", "n3")));
    String phase6Name = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phase6Name, phase6);

    Set<Connection> phaseConnections = new HashSet<>();
    phaseConnections.add(new Connection(phase1Name, phase2Name));
    phaseConnections.add(new Connection(phase2Name, phase3Name, true));
    phaseConnections.add(new Connection(phase2Name, phase4Name, false));
    phaseConnections.add(new Connection(phase3Name, phase5Name, true));
    phaseConnections.add(new Connection(phase4Name, phase6Name, true));

    PipelinePlan expected = new PipelinePlan(phases, phaseConnections);
    PipelinePlan actual = planner.plan(pipelineSpec);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMultipleActionConditions() {
    /*
                                                   |-- n2 -- a3
            |-- a1 --|        |-- n0 -- n1 -- c1 --|                          |-- a5 --|
        a0--|        |-- c0 --|                    |-- n3 -- c2 -- n8 -- a4 --|        |-- a7
            |-- a2 --|        |                                               |-- a6 --|
                              |        |-- n4 -- n5 -- c4 -- c5 -- n9
                              |-- c3 --|
                                       |              |-- a8
                                       |-- n6 -- n7 --|
                                                      |-- a9
     */
    Set<StageSpec> stageSpecs = ImmutableSet.of(
      StageSpec.builder("a0", ACTION).build(),
      StageSpec.builder("a1", ACTION).build(),
      StageSpec.builder("a2", ACTION).build(),
      StageSpec.builder("a3", ACTION).build(),
      StageSpec.builder("a4", ACTION).build(),
      StageSpec.builder("a5", ACTION).build(),
      StageSpec.builder("a6", ACTION).build(),
      StageSpec.builder("a7", ACTION).build(),
      StageSpec.builder("a8", ACTION).build(),
      StageSpec.builder("a9", ACTION).build(),
      StageSpec.builder("c0", CONDITION).build(),
      StageSpec.builder("c1", CONDITION).build(),
      StageSpec.builder("c2", CONDITION).build(),
      StageSpec.builder("c3", CONDITION).build(),
      StageSpec.builder("c4", CONDITION).build(),
      StageSpec.builder("c5", CONDITION).build(),
      StageSpec.builder("n0", NODE).build(),
      StageSpec.builder("n1", NODE).build(),
      StageSpec.builder("n2", NODE).build(),
      StageSpec.builder("n3", NODE).build(),
      StageSpec.builder("n4", NODE).build(),
      StageSpec.builder("n5", NODE).build(),
      StageSpec.builder("n6", NODE).build(),
      StageSpec.builder("n7", NODE).build(),
      StageSpec.builder("n8", NODE).build(),
      StageSpec.builder("n9", NODE).build()
    );

    Set<Connection> connections = ImmutableSet.of(
      new Connection("a0", "a1"),
      new Connection("a0", "a2"),
      new Connection("a1", "c0"),
      new Connection("a2", "c0"),
      new Connection("c0", "n0", true),
      new Connection("c0", "c3", false),
      new Connection("n0", "n1"),
      new Connection("n1", "c1"),
      new Connection("c1", "n2", true),
      new Connection("c1", "n3", false),
      new Connection("n2", "a3"),
      new Connection("n3", "c2"),
      new Connection("c2", "n8", true),
      new Connection("n8", "a4"),
      new Connection("a4", "a5"),
      new Connection("a4", "a6"),
      new Connection("a5", "a7"),
      new Connection("a6", "a7"),
      new Connection("c3", "n4", true),
      new Connection("c3", "n6", false),
      new Connection("n4", "n5"),
      new Connection("n5", "c4"),
      new Connection("c4", "c5", true),
      new Connection("c5", "n9", true),
      new Connection("n6", "n7"),
      new Connection("n7", "a8"),
      new Connection("n7", "a9"));

    Set<String> pluginTypes = ImmutableSet.of(NODE.getType(), ACTION.getType(), Constants.Connector.PLUGIN_TYPE,
                                              CONDITION.getType());
    Set<String> reduceTypes = ImmutableSet.of();
    Set<String> emptySet = ImmutableSet.of();
    Set<String> actionTypes = ImmutableSet.of(ACTION.getType());
    PipelinePlanner planner = new PipelinePlanner(pluginTypes, reduceTypes, emptySet, actionTypes, emptySet);
    PipelineSpec pipelineSpec = PipelineSpec.builder().addStages(stageSpecs).addConnections(connections).build();

    Map<String, PipelinePhase> phases = new HashMap<>();

    Set<Connection> phaseConnections = new HashSet<>();
    phaseConnections.add(new Connection("a0", "a1"));
    phaseConnections.add(new Connection("a0", "a2"));
    phaseConnections.add(new Connection("a1", "c0"));
    phaseConnections.add(new Connection("a2", "c0"));
    phaseConnections.add(new Connection("a0", "a1"));
    phaseConnections.add(new Connection("a0", "a1"));
    phaseConnections.add(new Connection("a4", "a5"));
    phaseConnections.add(new Connection("a4", "a6"));
    phaseConnections.add(new Connection("a5", "a7"));
    phaseConnections.add(new Connection("a6", "a7"));
    phaseConnections.add(new Connection("c0", "c3", false));
    phaseConnections.add(new Connection("c4", "c5", true));

    for (String action : ImmutableList.of("a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9")) {
      phases.put(action,
                 PipelinePhase.builder(pluginTypes)
                   .addStage(StageSpec.builder(action, ACTION).build())
                   .build());
    }
    for (String condition : ImmutableList.of("c0", "c1", "c2", "c3", "c4", "c5")) {
      phases.put(condition,
                 PipelinePhase.builder(pluginTypes)
                   .addStage(StageSpec.builder(condition, CONDITION).build())
                   .build());
    }

    // [c0] --true-->  [c0 -- n0 -- n1 -- c1]
    PipelinePhase phase = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n0", NODE).build())
      .addStage(StageSpec.builder("n1", NODE).build())
      .addStage(StageSpec.builder("c1.connector",
                                  connectorSpec("c1.connector", Constants.Connector.SINK_TYPE)).build())
      .addConnection("n0", "n1")
      .addConnection("n1", "c1.connector")
      .build();
    Dag controlPhaseDag =
      new Dag(ImmutableSet.of(new Connection("c0", "n0"), new Connection("n0", "n1"), new Connection("n1", "c1")));
    String phaseName = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phaseName, phase);
    phaseConnections.add(new Connection("c0", phaseName, true));
    // [c0 -- n0 -- n1 -- c1] --> [c1]
    phaseConnections.add(new Connection(phaseName, "c1"));


    // [c1] --true--> [c1 -- n2 -- a3]
    phase = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("c1.connector",
                                  connectorSpec("c1.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n2", NODE).build())
      .addConnection("c1.connector", "n2")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("c1", "n2"), new Connection("n2", "a3")));
    phaseName = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phaseName, phase);
    phaseConnections.add(new Connection("c1", phaseName, true));
    // [c1 -- n2 -- a3] -- [a3]
    phaseConnections.add(new Connection(phaseName, "a3"));


    // [c1] --false--> [c1 -- n3 -- c2]
    phase = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("c1.connector",
                                  connectorSpec("c1.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n3", NODE).build())
      .addStage(StageSpec.builder("c2.connector",
                                  connectorSpec("c2.connector", Constants.Connector.SINK_TYPE)).build())
      .addConnection("c1.connector", "n3")
      .addConnection("n3", "c2.connector")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("c1", "n3"), new Connection("n3", "c2")));
    phaseName = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phaseName, phase);
    phaseConnections.add(new Connection("c1", phaseName, false));
    // [c1.connector -- n3 -- c2.connector] --> [c2]
    phaseConnections.add(new Connection(phaseName, "c2"));


    // [c2] --true--> [c2 -- n8 -- a4]
    phase = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("c2.connector",
                                  connectorSpec("c2.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n8", NODE).build())
      .addConnection("c2.connector", "n8")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("c2", "n8"), new Connection("n8", "a4")));
    phaseName = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phaseName, phase);
    phaseConnections.add(new Connection("c2", phaseName, true));
    // [c2 -- n8 -- a4] --> [a4]
    phaseConnections.add(new Connection(phaseName, "a4"));


    // [c3] --true--> [c3 -- n4 -- n5 -- c4]
    phase = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n4", NODE).build())
      .addStage(StageSpec.builder("n5", NODE).build())
      .addStage(StageSpec.builder("c4.connector",
                                  connectorSpec("c4.connector", Constants.Connector.SINK_TYPE)).build())
      .addConnection("n4", "n5")
      .addConnection("n5", "c4.connector")
      .build();
    controlPhaseDag =
      new Dag(ImmutableSet.of(new Connection("c3", "n4"), new Connection("n4", "n5"), new Connection("n5", "c4")));
    phaseName = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phaseName, phase);
    phaseConnections.add(new Connection("c3", phaseName, true));
    // [c3 -- n4 -- n5 -- c4] --> c4
    phaseConnections.add(new Connection(phaseName, "c4"));


    // [c5] --true--> [c5 (via c4.connector) -- n9]
    phase = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("c4.connector",
                                  connectorSpec("c4.connector", Constants.Connector.SOURCE_TYPE)).build())
      .addStage(StageSpec.builder("n9", NODE).build())
      .addConnection("c4.connector", "n9")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(new Connection("c5", "n9")));
    phaseName = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phaseName, phase);
    phaseConnections.add(new Connection("c5", phaseName, true));


    // [c3] --false--> [c3 -- n6 -- n7 -- a8, a9]
    phase = PipelinePhase.builder(pluginTypes)
      .addStage(StageSpec.builder("n6", NODE).build())
      .addStage(StageSpec.builder("n7", NODE).build())
      .addConnection("n6", "n7")
      .build();
    controlPhaseDag = new Dag(ImmutableSet.of(
      new Connection("c3", "n6"), new Connection("n6", "n7"), new Connection("n7", "a8"), new Connection("n7", "a9")));
    phaseName = PipelinePlanner.getPhaseName(controlPhaseDag);
    phases.put(phaseName, phase);
    phaseConnections.add(new Connection("c3", phaseName, false));
    // [c3 -- n6 -- n7 -- a8, a9] --> [a8]
    // [c3 -- n6 -- n7 -- a8, a9] --> [a9]
    phaseConnections.add(new Connection(phaseName, "a8"));
    phaseConnections.add(new Connection(phaseName, "a9"));

    PipelinePlan expected = new PipelinePlan(phases, phaseConnections);
    PipelinePlan actual = planner.plan(pipelineSpec);
    Assert.assertEquals(expected, actual);
  }

  private static PluginSpec connectorSpec(String originalName, String type) {
    return new PluginSpec(Constants.Connector.PLUGIN_TYPE, "connector",
                          ImmutableMap.of(Constants.Connector.TYPE, type,
                                          Constants.Connector.ORIGINAL_NAME, originalName), null);
  }
}
