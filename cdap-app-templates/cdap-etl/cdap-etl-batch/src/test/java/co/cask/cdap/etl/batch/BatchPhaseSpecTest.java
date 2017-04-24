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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

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
    PipelinePhase.Builder builder =
      PipelinePhase.builder(ImmutableSet.of(BatchSource.PLUGIN_TYPE, Constants.CONNECTOR_TYPE))
        .addStage(StageInfo.builder("source1", BatchSource.PLUGIN_TYPE).build())
        .addStage(StageInfo.builder("source2", BatchSource.PLUGIN_TYPE)
                    .addInputSchema("a", Schema.recordOf("stuff", Schema.Field.of("x", Schema.of(Schema.Type.INT))))
                    .build())
        .addStage(StageInfo.builder("sink.connector", Constants.CONNECTOR_TYPE).build())
        .addConnection("source1", "sink.connector")
        .addConnection("source2", "sink.connector");

    BatchPhaseSpec phaseSpec =
      new BatchPhaseSpec("phase-1", builder.build(), new Resources(), new Resources(), new Resources(),
                         false, false, Collections.<String, String>emptyMap(),
                         0, Collections.<String, String>emptyMap());
    Assert.assertEquals("Sources 'source1', 'source2' to sinks 'sink.connector'.", phaseSpec.getDescription());
  }
}
