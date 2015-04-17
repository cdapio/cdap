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

package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.MockAdapterConfigurer;
import co.cask.cdap.templates.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.templates.etl.realtime.sinks.NoOpSink;
import co.cask.cdap.templates.etl.realtime.sources.TestSource;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkerManager;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLRealtimeTemplate}.
 */
public class ETLWorkerTest extends TestBase {
  private static ApplicationManager templateManager;

  @BeforeClass
  public static void setupTests() {
    templateManager = deployApplication(ETLRealtimeTemplate.class);
  }

  // TODO: Replace this test with meaningful sources and sinks once they are available.
  @Test
  public void testSimpleConfig() throws Exception {
    ApplicationTemplate<ETLRealtimeConfig> appTemplate = new ETLRealtimeTemplate();

    ETLStage source = new ETLStage(TestSource.class.getSimpleName(), ImmutableMap.<String, String>of());
    ETLStage sink = new ETLStage(NoOpSink.class.getSimpleName(), ImmutableMap.<String, String>of());
    List<ETLStage> transforms = Lists.newArrayList();
    ETLRealtimeConfig adapterConfig = new ETLRealtimeConfig(1, source, sink, transforms);
    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);

    Map<String, String> workerArgs = Maps.newHashMap();
    for (Map.Entry<String, String> entry : adapterConfigurer.getArguments().entrySet()) {
      workerArgs.put(entry.getKey(), entry.getValue());
    }

    WorkerManager workerManager = templateManager.startWorker(ETLWorker.class.getSimpleName(), workerArgs);
    TimeUnit.SECONDS.sleep(5);
    workerManager.stop();
    templateManager.stopAll();
  }
}
