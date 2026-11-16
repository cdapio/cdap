/*
 *  Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.submit;

import io.cdap.cdap.app.runtime.spark.Constant;
import io.cdap.cdap.app.runtime.spark.MockMasterEnvironment;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextTest;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeEnv;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.app.runtime.spark.distributed.SparkExecutionClient;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import io.cdap.cdap.master.spi.environment.spark.SparkDriverWatcher;
import io.cdap.cdap.master.spi.environment.spark.SparkSubmitContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Tests for MasterEnvironment Spark submission.
 */
public class MasterEnvironmentSparkSubmitterTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testExtraJavaOptionsAppended() {
    String envOption = "-Dextra.java.opt=0";
    SparkConfig sparkConfig = new SparkConfig("master", URI.create("local://0"),
        Collections.emptyMap(), null, envOption);

    Map<String, String> appConf = new HashMap<>();
    String driverOption = "-Dapp.opt=driver";
    String executorOption = "-Dapp.opt=executor";
    appConf.put("spark.driver.extraJavaOptions", "-Dapp.opt=driver");
    appConf.put("spark.executor.extraJavaOptions", "-Dapp.opt=executor");
    Map<String, String> expected = new HashMap<>();
    expected.put("spark.driver.extraJavaOptions", envOption + " " + driverOption);
    expected.put("spark.executor.extraJavaOptions", envOption + " " + executorOption);

    Map<String, String> merged =
        MasterEnvironmentSparkSubmitter.mergeSparkConfs(appConf, sparkConfig);
    Assert.assertEquals(expected, merged);
  }

  @Test
  public void testBeforeSubmitTriggerShutdownAndOnCompleted() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constant.Spark.ArtifactFetcher.PORT, 12345);
    SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(cConf, new Configuration());

    final SparkDriverWatcher watcher = Mockito.mock(SparkDriverWatcher.class);
    MasterEnvironment masterEnv = new MockMasterEnvironment() {
      @Override
      public SparkConfig generateSparkSubmitConfig(SparkSubmitContext context) {
        return new SparkConfig("k8s", URI.create("local:///job.jar"), Collections.<String, String>emptyMap(),
                               watcher, null);
      }
    };

    MasterEnvironmentSparkSubmitter submitter = new MasterEnvironmentSparkSubmitter(
        cConf, locationFactory, InetAddress.getLoopbackAddress().getCanonicalHostName(),
        runtimeContext, masterEnv, runtimeContext.getProgramOptions());
    submitter.getFiles(Collections.<LocalizeResource>emptyList());
    submitter.beforeSubmit();

    Mockito.verify(watcher).initialize();
    String serviceUriStr = SparkRuntimeEnv.getProperty(
        SparkConfig.DRIVER_ENV_PREFIX + SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI);
    Assert.assertNotNull(serviceUriStr);

    SparkExecutionClient client = new SparkExecutionClient(
        URI.create(serviceUriStr), runtimeContext.getProgramRunId());
    Assert.assertNull(client.heartbeat(null));

    client.completed(null);
    submitter.triggerShutdown(5L, TimeUnit.SECONDS);
    try {
      client.heartbeat(null);
      Assert.fail("Expected IOException after triggerShutdown");
    } catch (IOException expected) {
      // expected
    }

    MasterEnvironmentSparkSubmitter submitter2 = new MasterEnvironmentSparkSubmitter(
        cConf, locationFactory, InetAddress.getLoopbackAddress().getCanonicalHostName(),
        runtimeContext, masterEnv, runtimeContext.getProgramOptions());
    submitter2.getFiles(Collections.<LocalizeResource>emptyList());
    submitter2.beforeSubmit();
    submitter2.onCompleted(true);
    Mockito.verify(watcher).close();
  }
}
