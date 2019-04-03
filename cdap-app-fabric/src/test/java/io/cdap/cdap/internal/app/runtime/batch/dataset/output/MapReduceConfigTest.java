/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.internal.app.runtime.batch.dataset.output;

import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.batch.MapReduceRunnerTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests correct configuration for map task context
 */
public class MapReduceConfigTest extends MapReduceRunnerTestBase {

  @Test
  public void testConfigIsolation() throws Exception {
    ApplicationWithPrograms app = deployApp(AppWithSingleInputOutput.class);
    Assert.assertTrue(runProgram(app, AppWithSingleInputOutput.SimpleMapReduce.class, new BasicArguments()));
  }
}
