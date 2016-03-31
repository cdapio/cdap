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

package co.cask.cdap.etl.mock.test;

import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.mock.batch.aggregator.FieldCountAggregator;
import co.cask.cdap.etl.mock.batch.aggregator.IdentityAggregator;
import co.cask.cdap.etl.mock.realtime.LookupSource;
import co.cask.cdap.etl.mock.realtime.MockSink;
import co.cask.cdap.etl.mock.realtime.MockSource;
import co.cask.cdap.etl.mock.transform.DoubleTransform;
import co.cask.cdap.etl.mock.transform.ErrorTransform;
import co.cask.cdap.etl.mock.transform.IdentityTransform;
import co.cask.cdap.etl.mock.transform.IntValueFilterTransform;
import co.cask.cdap.etl.mock.transform.StringValueFilterTransform;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.TestBase;

/**
 * Performs common setup logic
 */
public class HydratorTestBase extends TestBase {

  public HydratorTestBase() {
  }

  protected static void setupRealtimeArtifacts(Id.Artifact artifactId, Class<?> appClass) throws Exception {
    addAppArtifact(artifactId, appClass,
                   RealtimeSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, artifactId.getName() + "-mocks", "1.0.0"), artifactId,
                      MockSink.class, MockSource.class, LookupSource.class,
                      DoubleTransform.class, ErrorTransform.class, IdentityTransform.class,
                      IntValueFilterTransform.class, StringValueFilterTransform.class);
  }

  protected static void setupBatchArtifacts(Id.Artifact artifactId, Class<?> appClass) throws Exception {
    // add the app artifact
    addAppArtifact(artifactId, appClass,
                   BatchSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName(),
                   "org.apache.avro.mapred", "org.apache.avro", "org.apache.avro.generic", "org.apache.avro.io");

    // add plugins artifact
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, artifactId.getName() + "-mocks", "1.0.0"), artifactId,
                      co.cask.cdap.etl.mock.batch.MockSource.class,
                      co.cask.cdap.etl.mock.batch.MockSink.class,
                      DoubleTransform.class, ErrorTransform.class, IdentityTransform.class,
                      IntValueFilterTransform.class, StringValueFilterTransform.class,
                      FieldCountAggregator.class, IdentityAggregator.class);
  }

}
