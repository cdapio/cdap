/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.distributed;

import co.cask.cdap.app.runtime.spark.SparkProgramRunner;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.test.MockTwillContext;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.inject.Guice;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Tests for guice module used in {@link SparkTwillRunnable}.
 */
public class SparkTwillRunnableModuleTest {

  @Test
  public void testSpark() {
    ProgramId programId = NamespaceId.DEFAULT.app("test").spark("spark");
    Module module = new SparkTwillRunnable("spark").createModule(CConfiguration.create(), new Configuration(),
                                                                 new MockTwillContext(), programId,
                                                                 RunIds.generate().getId(), "0", "principal");
    Guice.createInjector(module).getInstance(SparkProgramRunner.class);
  }
}
