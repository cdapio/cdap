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
 *
 */

package io.cdap.cdap.etl.proto.v2.spec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests for StageSpec
 */
public class StageSpecTest {
    private static final ArtifactId ARTIFACT_ID =
            new ArtifactId("plugins", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    private static final Schema SCHEMA_A = Schema.recordOf("a", Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    private static final Schema SCHEMA_B = Schema.recordOf("b", Schema.Field.of("b", Schema.of(Schema.Type.STRING)));

    @Test
    public void testInputStagesNotSet() {
        StageSpec spec = StageSpec.builder("sink",
                new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", ImmutableMap.of(), ARTIFACT_ID))
                .addInputSchema("source", SCHEMA_A).build();
        Assert.assertEquals(spec.getInputStages(), ImmutableList.of("source"));
    }

    @Test
    public void testEquality() {
        StageSpec spec1 = StageSpec.builder("sink",
                new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", ImmutableMap.of(), ARTIFACT_ID))
                .addInputSchema("source1", SCHEMA_A)
                .addInputSchema("source2", SCHEMA_B)
                .addInputStages(ImmutableList.of("source1", "source2"))
                .build();

        StageSpec spec2 = StageSpec.builder("sink",
                new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", ImmutableMap.of(), ARTIFACT_ID))
                .addInputSchema("source1", SCHEMA_A)
                .addInputSchema("source2", SCHEMA_B)
                .addInputStage("source2")
                .addInputStage("source1")
                .build();

        Assert.assertEquals(spec1, spec2);
    }

    @Test
    public void testHashCode() {
        StageSpec spec1 = StageSpec.builder("sink",
                new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", ImmutableMap.of(), ARTIFACT_ID))
                .addInputSchemas(ImmutableMap.of("source1", SCHEMA_A, "source2", SCHEMA_B))
                .addInputStage("source1")
                .addInputStage("source2")
                .build();

        StageSpec spec2 = StageSpec.builder("sink",
                new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", ImmutableMap.of(), ARTIFACT_ID))
                .addInputSchema("source1", SCHEMA_A)
                .addInputSchema("source2", SCHEMA_B)
                .addInputStages(ImmutableList.of("source2", "source1"))
                .build();

        Assert.assertEquals(spec1.hashCode(), spec2.hashCode());
    }

    @Test
    public void testHashCodeWithoutInputStages() {
        // This can happen if JSON generated with old code is deserialized with newer code.
        StageSpec spec1 = StageSpec.builder("sink",
            new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", ImmutableMap.of(), ARTIFACT_ID))
            .addInputSchemas(ImmutableMap.of("source1", SCHEMA_A, "source2", SCHEMA_B))
            .build();

        StageSpec spec2 = StageSpec.builder("sink",
            new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", ImmutableMap.of(), ARTIFACT_ID))
            .addInputSchema("source1", SCHEMA_A)
            .addInputSchema("source2", SCHEMA_B)
            .addInputStages(ImmutableList.of("source1", "source2"))
            .build();

        Assert.assertEquals(spec1.hashCode(), spec2.hashCode());
    }

    @Test
    public void testEqualityWithoutInputStages() {
        StageSpec spec1 = StageSpec.builder("sink",
            new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", ImmutableMap.of(), ARTIFACT_ID))
            .addInputSchema("source1", SCHEMA_A)
            .addInputSchema("source2", SCHEMA_B)
            .addInputStages(ImmutableList.of("source1", "source2"))
            .build();

        // This can happen if JSON generated with old code is deserialized with newer code.
        StageSpec spec2 = StageSpec.builder("sink",
            new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", ImmutableMap.of(), ARTIFACT_ID))
            .addInputSchema("source1", SCHEMA_A)
            .addInputSchema("source2", SCHEMA_B)
            .build();

        Assert.assertEquals(spec1, spec2);
    }

}
