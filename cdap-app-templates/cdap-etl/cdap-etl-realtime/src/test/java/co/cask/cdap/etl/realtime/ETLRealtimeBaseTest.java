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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.realtime.mock.MockSink;
import co.cask.cdap.etl.realtime.mock.MockSource;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.TestBase;
import org.junit.BeforeClass;

/**
 * Performs common setup logic
 */
public class ETLRealtimeBaseTest extends TestBase {
  protected static final Id.Artifact APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT, "etlrealtime", "3.2.0");
  protected static final ArtifactSummary APP_ARTIFACT = ArtifactSummary.from(APP_ARTIFACT_ID);

  @BeforeClass
  public static void setupTests() throws Exception {
    addAppArtifact(APP_ARTIFACT_ID, ETLRealtimeApplication.class,
                   RealtimeSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "mock-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      MockSink.class, MockSource.class);
  }
}
