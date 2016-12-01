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

package co.cask.cdap.operations.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Ignore;

import java.io.IOException;

/**
 * Tests operational stats with a non-HA Resource Manager
 */
public class YarnOperationalStatsTest extends AbstractYarnOperationalStatsTest {
  @Override
  protected MiniYARNCluster createYarnCluster() throws IOException, InterruptedException, YarnException {
    MiniYARNCluster yarnCluster = new MiniYARNCluster(getClass().getName(), 1, 1, 1);
    yarnCluster.init(new Configuration());
    yarnCluster.start();
    return yarnCluster;
  }

  @Override
  protected int getNumNodes() {
    return 1;
  }

  @Ignore
  @Override
  public void test() throws Exception {
    // TODO: CDAP-7726 Fix flaky test
  }
}
