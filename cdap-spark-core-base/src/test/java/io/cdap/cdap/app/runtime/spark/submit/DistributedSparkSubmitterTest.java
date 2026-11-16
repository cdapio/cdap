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

package io.cdap.cdap.app.runtime.spark.submit;

import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextTest;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeEnv;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.app.runtime.spark.distributed.SparkExecutionClient;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.runtime.spi.runtimejob.LaunchMode;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link DistributedSparkSubmitter}.
 */
public class DistributedSparkSubmitterTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testBeforeSubmitAndTriggerShutdown() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Configuration hConf = new Configuration();
    SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(
        CConfiguration.create(), hConf);

    DistributedSparkSubmitter submitter = new DistributedSparkSubmitter(
        hConf, locationFactory, InetAddress.getLoopbackAddress().getCanonicalHostName(),
        runtimeContext, "default", LaunchMode.CLUSTER);

    submitter.beforeSubmit();
    String serviceUriStr = SparkRuntimeEnv.getProperty(
        "spark.yarn.appMasterEnv." + SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI);
    Assert.assertNotNull(serviceUriStr);

    SparkExecutionClient client = new SparkExecutionClient(
        URI.create(serviceUriStr), runtimeContext.getProgramRunId());
    Assert.assertNull(client.heartbeat(null));

    client.completed(null);
    submitter.triggerShutdown(5L, TimeUnit.SECONDS);

    try {
      client.heartbeat(null);
      Assert.fail("Expected IOException after SparkExecutionService stopped");
    } catch (IOException expected) {
      // expected
    }
  }

  @Test
  public void testBeforeSubmitAndOnCompletedStopsServiceImmediately() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Configuration hConf = new Configuration();
    SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(
        CConfiguration.create(), hConf);

    DistributedSparkSubmitter submitter = new DistributedSparkSubmitter(
        hConf, locationFactory, InetAddress.getLoopbackAddress().getCanonicalHostName(),
        runtimeContext, null, LaunchMode.CLIENT);

    submitter.beforeSubmit();
    String serviceUriStr = SparkRuntimeEnv.getProperty(
        "spark.yarn.appMasterEnv." + SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI);
    SparkExecutionClient client = new SparkExecutionClient(
        URI.create(serviceUriStr), runtimeContext.getProgramRunId());
    Assert.assertNull(client.heartbeat(null));

    submitter.onCompleted(true);
    try {
      client.heartbeat(null);
      Assert.fail("Expected IOException after onCompleted shut down SparkExecutionService");
    } catch (IOException expected) {
      // expected
    }
  }
}
