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

package co.cask.cdap.app.guice;

import co.cask.cdap.common.conf.CConfiguration;
import com.google.inject.Guice;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Test case that simple creates a Guice injector from the {@link DistributedProgramRunnableModule}, to ensure
 * that all bindings are proper.
 */
public class DistributedProgramRunnableModuleTest {
  @Test
  public void createModule() throws Exception {
    DistributedProgramRunnableModule distributedProgramRunnableModule =
      new DistributedProgramRunnableModule(CConfiguration.create(), new Configuration());
    Guice.createInjector(distributedProgramRunnableModule.createModule());
  }
}
