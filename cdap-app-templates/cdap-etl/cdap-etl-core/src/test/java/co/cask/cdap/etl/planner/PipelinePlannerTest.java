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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
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
    PluginSpec sourcePlugin = new PluginSpec(BatchSource.PLUGIN_TYPE, "mock", empty, artifactId);
    PluginSpec reducePlugin = new PluginSpec(AGGREGATOR, "mock", empty, artifactId);
    PluginSpec transformPlugin = new PluginSpec(Transform.PLUGIN_TYPE, "mock", empty, artifactId);
    PluginSpec sinkPlugin = new PluginSpec(BatchSink.PLUGIN_TYPE, "mock", empty, artifactId);
    Schema schema = Schema.recordOf("stuff", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Set<StageSpec> stageSpecs = ImmutableSet.of(
      StageSpec.builder("n1", sourcePlugin)
        .setOutputSchema(schema)
        .addOutputs("n2", "n3", "n4")
        .build(),
      StageSpec.builder("n2", reducePlugin)
        .setInputSchema(schema)
        .setOutputSchema(schema)
        .addInputs("n1")
        .addOutputs("n6")
        .build(),
      StageSpec.builder("n3", reducePlugin)
        .setInputSchema(schema)
        .setOutputSchema(schema)
        .addInputs("n1")
        .addOutputs("n5")
        .build(),
      StageSpec.builder("n4", reducePlugin)
        .setInputSchema(schema)
        .setOutputSchema(schema)
        .addInputs("n1")
        .addOutputs("n6")
        .build(),
      StageSpec.builder("n5", transformPlugin)
        .setInputSchema(schema)
        .setOutputSchema(schema)
        .addInputs("n3")
        .addOutputs("n6")
        .build(),
      StageSpec.builder("n6", transformPlugin)
        .setInputSchema(schema)
        .setOutputSchema(schema)
        .addInputs("n2", "n5", "n4")
        .addOutputs("n7")
        .build(),
      StageSpec.builder("n7", reducePlugin)
        .setInputSchema(schema)
        .setOutputSchema(schema)
        .addInputs("n6")
        .addOutputs("n8")
        .build(),
      StageSpec.builder("n8", transformPlugin)
        .setInputSchema(schema)
        .setOutputSchema(schema)
        .addInputs("n7")
        .addOutputs("n9")
        .build(),
      StageSpec.builder("n9", reducePlugin)
        .setInputSchema(schema)
        .setOutputSchema(schema)
        .addInputs("n8")
        .addOutputs("n10", "n11")
        .build(),
      StageSpec.builder("n10", sinkPlugin)
        .setInputSchema(schema)
        .addInputs("n9")
        .build(),
      StageSpec.builder("n11", sinkPlugin)
        .setInputSchema(schema)
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

    PipelinePlanner planner = new PipelinePlanner(BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE,
                                                  ImmutableSet.of(AGGREGATOR));
    PipelineSpec pipelineSpec = new PipelineSpec(stageSpecs, connections, new Resources(), true);

    Map<String, PipelinePhase> phases = new HashMap<>();
    /*
             |--- n2.connector
             |
        n1 --|--- n3.connector
             |
             |--- n4.connector
     */
    PipelinePhase phase1 = new PipelinePhase(
      new StageInfo("n1", null, false),
      null,
      ImmutableSet.of(
        new StageInfo("n2.connector", null, true),
        new StageInfo("n3.connector", null, true),
        new StageInfo("n4.connector", null, true)
      ),
      ImmutableSet.<StageInfo>of(),
      ImmutableMap.<String, Set<String>>of(
        "n1", ImmutableSet.of("n2.connector", "n3.connector", "n4.connector")
      )
    );
    String phase1Name = getPhaseName("n1", "n2.connector", "n3.connector", "n4.connector");
    phases.put(phase1Name, phase1);

    /*
        phase2:
        n2.connector --- n2(r) --- n6.connector
     */
    PipelinePhase phase2 = new PipelinePhase(
      new StageInfo("n2.connector", null, true),
      new StageInfo("n2", null, false),
      ImmutableSet.of(new StageInfo("n6.connector", null, true)),
      ImmutableSet.<StageInfo>of(),
      ImmutableMap.<String, Set<String>>of(
        "n2.connector", ImmutableSet.of("n2"),
        "n2", ImmutableSet.of("n6.connector")
      )
    );
    String phase2Name = getPhaseName("n2.connector", "n6.connector");
    phases.put(phase2Name, phase2);

    /*
        phase3:
        n3.connector --- n3(r) --- n5 --- n6.connector
     */
    PipelinePhase phase3 = new PipelinePhase(
      new StageInfo("n3.connector", null, true),
      new StageInfo("n3", null, false),
      ImmutableSet.of(new StageInfo("n6.connector", null, true)),
      ImmutableSet.of(new StageInfo("n5", null, false)),
      ImmutableMap.<String, Set<String>>of(
        "n3.connector", ImmutableSet.of("n3"),
        "n3", ImmutableSet.of("n5"),
        "n5", ImmutableSet.of("n6.connector")
      )
    );
    String phase3Name = getPhaseName("n3.connector", "n6.connector");
    phases.put(phase3Name, phase3);

    /*
        phase4:
        n4.connector --- n4(r) --- n6.connector
     */
    PipelinePhase phase4 = new PipelinePhase(
      new StageInfo("n4.connector", null, true),
      new StageInfo("n4", null, false),
      ImmutableSet.of(new StageInfo("n6.connector", null, true)),
      ImmutableSet.<StageInfo>of(),
      ImmutableMap.<String, Set<String>>of(
        "n4.connector", ImmutableSet.of("n4"),
        "n4", ImmutableSet.of("n6.connector")
      )
    );
    String phase4Name = getPhaseName("n4.connector", "n6.connector");
    phases.put(phase4Name, phase4);

    /*
        phase5:
        n6.connector --- n6 --- n7(r) --- n8 --- n9.connector
     */
    PipelinePhase phase5 = new PipelinePhase(
      new StageInfo("n6.connector", null, true),
      new StageInfo("n7", null, false),
      ImmutableSet.of(new StageInfo("n9.connector", null, true)),
      ImmutableSet.of(new StageInfo("n6", null, false), new StageInfo("n8", null, false)),
      ImmutableMap.<String, Set<String>>of(
        "n6.connector", ImmutableSet.of("n6"),
        "n6", ImmutableSet.of("n7"),
        "n7", ImmutableSet.of("n8"),
        "n8", ImmutableSet.of("n9.connector")
      )
    );
    String phase5Name = getPhaseName("n6.connector", "n9.connector");
    phases.put(phase5Name, phase5);

    /*
        phase6:
                                 |-- n10
        n9.connector --- n9(r) --|
                                 |-- n11
     */
    PipelinePhase phase6 = new PipelinePhase(
      new StageInfo("n9.connector", null, true),
      new StageInfo("n9", null, false),
      ImmutableSet.of(new StageInfo("n10", null, false), new StageInfo("n11", null, false)),
      ImmutableSet.<StageInfo>of(),
      ImmutableMap.<String, Set<String>>of(
        "n9.connector", ImmutableSet.of("n9"),
        "n9", ImmutableSet.of("n10", "n11")
      )
    );
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
