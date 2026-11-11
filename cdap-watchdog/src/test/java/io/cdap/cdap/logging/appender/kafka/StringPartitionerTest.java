/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.kafka;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link StringPartitioner}.
 */
public class StringPartitionerTest {

  @Test
  public void testFixedKeyToPartitionMapping() {
    CConfiguration cConf8 = CConfiguration.create();
    cConf8.setInt(Constants.Logging.NUM_PARTITIONS, 8);
    StringPartitioner partitioner8 = new StringPartitioner(cConf8);

    Assert.assertEquals(3, partitioner8.partition("default:MyApp:Workflow:DataPipelineWorkflow", 8));
    Assert.assertEquals(1, partitioner8.partition("system:services:appfabric", 8));
    Assert.assertEquals(6, partitioner8.partition("ns1:etl-app:spark:Phase-1", 8));
    Assert.assertEquals(0, partitioner8.partition("ns_üñîçødé:app:worker:w1", 8));

    CConfiguration cConf16 = CConfiguration.create();
    cConf16.setInt(Constants.Logging.NUM_PARTITIONS, 16);
    StringPartitioner partitioner16 = new StringPartitioner(cConf16);

    Assert.assertEquals(11, partitioner16.partition("default:MyApp:Workflow:DataPipelineWorkflow", 16));
    Assert.assertEquals(1, partitioner16.partition("system:services:appfabric", 16));
    Assert.assertEquals(6, partitioner16.partition("ns1:etl-app:spark:Phase-1", 16));
    Assert.assertEquals(8, partitioner16.partition("ns_üñîçødé:app:worker:w1", 16));
  }
}
