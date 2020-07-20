/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.distributed;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.spark.SparkProgramRunner;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextProvider;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.test.MockTwillContext;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ServiceAnnouncer;
import org.junit.Test;

/**
 * Tests for guice module used in {@link SparkTwillRunnable}.
 */
public class SparkTwillRunnableModuleTest {

  @Test
  public void testSpark() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").spark("spark").run(RunIds.generate());

    for (ClusterMode mode : ClusterMode.values()) {
      Module module = new SparkTwillRunnable("spark") {
        @Override
        protected ServiceAnnouncer getServiceAnnouncer() {
          return new MockTwillContext();
        }
      }.createModule(CConfiguration.create(), new Configuration(),
                     createProgramOptions(programRunId, mode), programRunId);
      Injector injector = Guice.createInjector(module);
      injector.getInstance(SparkProgramRunner.class);
      injector.getInstance(ExploreClient.class);

      Injector contextInjector = SparkRuntimeContextProvider.createInjector(CConfiguration.create(),
                                                                            new Configuration(),
                                                                            programRunId.getParent(),
                                                                            createProgramOptions(programRunId, mode));
      contextInjector.getInstance(PluginFinder.class);
      contextInjector.getInstance(ExploreClient.class);
    }
  }

  private ProgramOptions createProgramOptions(ProgramRunId programRunId, ClusterMode clusterMode) {
    return new SimpleProgramOptions(programRunId.getParent(),
                                    new BasicArguments(ImmutableMap.of(
                                      ProgramOptionConstants.INSTANCE_ID, "0",
                                      ProgramOptionConstants.PRINCIPAL, "principal",
                                      ProgramOptionConstants.RUN_ID, programRunId.getRun(),
                                      ProgramOptionConstants.CLUSTER_MODE, clusterMode.name())),
                                    new BasicArguments());
  }
}
