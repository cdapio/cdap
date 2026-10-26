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

package io.cdap.cdap.logging.appender.tms;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link TMSLogAppender} partition calculation.
 */
public class TMSLogAppenderTest {

  @Test
  public void testFixedKeyToPartitionMapping() {
    Assert.assertEquals(3, TMSLogAppender.partition("default:MyApp:Workflow:DataPipelineWorkflow", 8));
    Assert.assertEquals(1, TMSLogAppender.partition("system:services:appfabric", 8));
    Assert.assertEquals(6, TMSLogAppender.partition("ns1:etl-app:spark:Phase-1", 8));
    Assert.assertEquals(0, TMSLogAppender.partition("ns_üñîçødé:app:worker:w1", 8));

    Assert.assertEquals(11, TMSLogAppender.partition("default:MyApp:Workflow:DataPipelineWorkflow", 16));
    Assert.assertEquals(1, TMSLogAppender.partition("system:services:appfabric", 16));
    Assert.assertEquals(6, TMSLogAppender.partition("ns1:etl-app:spark:Phase-1", 16));
    Assert.assertEquals(8, TMSLogAppender.partition("ns_üñîçødé:app:worker:w1", 16));
  }
}
