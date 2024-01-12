/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.common;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DataprocMetric}.
 */
public class DataprocMetricTest {

  @Test
  public void testImageVersion() {
    String metricName = "provisioner.createCluster.response.count";
    String region = "us-east1";
    String imageVersion = "2.1.35-debian11";

    DataprocMetric dataprocMetric =
        DataprocMetric.builder(metricName)
            .setRegion(region).setImageVersion(imageVersion).build();
    Assert.assertEquals("2.1", dataprocMetric.getImageVersion());

    imageVersion = "2.1";
    dataprocMetric = DataprocMetric.builder(metricName)
        .setRegion(region).setImageVersion(imageVersion).build();
    Assert.assertEquals("2.1", dataprocMetric.getImageVersion());

    imageVersion = null;
    dataprocMetric = DataprocMetric.builder(metricName)
        .setRegion(region).setImageVersion(imageVersion).build();
    Assert.assertNull(dataprocMetric.getImageVersion());

    imageVersion = "2";
    dataprocMetric = DataprocMetric.builder(metricName)
        .setRegion(region).setImageVersion(imageVersion).build();
    Assert.assertEquals("2", dataprocMetric.getImageVersion());

  }
}
