/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.flow.stream;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(SlowTests.class)
public class FlowStreamIntegrationTestRun extends TestFrameworkTestBase {
  @Test
  public void testStreamBatch() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestFlowStreamIntegrationApp.class);
    StreamWriter s1 = applicationManager.getStreamWriter("s1");
    for (int i = 0; i < 50; i++) {
      s1.send(String.valueOf(i));
    }

    applicationManager.startFlow("StreamTestFlow");
    RuntimeMetrics flowletMetrics1 = RuntimeStats.getFlowletMetrics("TestFlowStreamIntegrationApp",
                                                                    "StreamTestFlow", "StreamReader");
    flowletMetrics1.waitForProcessed(1, 10, TimeUnit.SECONDS);
    if (flowletMetrics1.getException() > 0) {
      Assert.fail("StreamReader test failed");
    }
  }
}
