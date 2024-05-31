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

package io.cdap.cdap.master.startup;

import io.cdap.cdap.common.conf.CConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

/**
 * Test Guice injector for {@link MasterStartupTool}
 */
public class MasterStartupToolTest {

  @Test
  public void testInjector() throws Exception {
    MasterStartupTool.createInjector(CConfiguration.create(), HBaseConfiguration.create());
    // should not throw exception
  }
}
